package certmagicrados

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/caddyserver/certmagic"
	"github.com/ceph/go-ceph/rados"
	"go.uber.org/zap"
)

// Storage implements certmagic.Storage using Ceph's RADOS object store.
type Storage struct {
	io          *rados.IOContext
	oid         string
	lockTimeout time.Duration
	log         *zap.Logger

	locksMu sync.Mutex
	locks   map[string]*lock
}

// A StorageOption may be passed to NewStorage to customize the Storage's
// behaviour.
type StorageOption func(*Storage)

// WithLockTimeout sets up RADOS locks to expire after d has passed.
func WithLockTimeout(d time.Duration) StorageOption {
	return func(s *Storage) {
		s.lockTimeout = d
	}
}

func WithLogger(l *zap.Logger) StorageOption {
	return func(s *Storage) {
		s.log = l
	}
}

func NewStorage(ioctx *rados.IOContext, oid string, opts ...StorageOption) *Storage {
	s := &Storage{
		io:          ioctx,
		oid:         oid,
		lockTimeout: time.Minute,
		log:         zap.NewNop(),
		locks:       make(map[string]*lock),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *Storage) putLock(key string) *lock {
	l := &lock{
		stopMaintenance: make(chan struct{}),
	}

	defer s.locksMu.Unlock()
	s.locksMu.Lock()
	if s.locks[key] != nil {
		panic(fmt.Sprintf("putLock: already locked! (key=%q)", key))
	}
	s.locks[key] = l

	return l
}

func (s *Storage) dropLock(key string) *lock {
	s.locksMu.Lock()
	l := s.locks[key]
	delete(s.locks, key)
	s.locksMu.Unlock()

	if l != nil {
		close(l.stopMaintenance)
	}
	return l
}

type lock struct {
	stopMaintenance chan struct{}
}

const (
	lockCookie        = ""
	lockRetryInternal = time.Second
)
const (
	lockFlagMayRenew = 1 << iota
	lockFlagMustRenew
)

func lockName(key string) string {
	return "certmagicrados-" + key
}

func lockDesc(key string) string {
	return ""
}

func (s *Storage) Lock(ctx context.Context, key string) error {
	for {
		s.log.Debug("try to acquire lock", zap.String("key", key))
		r, err := s.io.LockExclusive(s.oid, lockName(key), lockCookie, lockDesc(key), s.lockTimeout, nil)
		if err != nil {
			return err
		}
		if r == 0 {
			s.log.Debug("acquired lock", zap.String("key", key))
			l := s.putLock(key)
			go s.maintainLock(key, l.stopMaintenance)
			return nil
		}
		s.log.Debug("lock busy", zap.String("key", key))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(lockRetryInternal):
			// BUG(ls): grow backoff duration with number of retries!
		}
	}
}

var errRenewalFailed = errors.New("failed to renew lock")

func (s *Storage) maintainLock(key string, stop <-chan struct{}) error {
	flags := uint8(lockFlagMustRenew)
	renew := time.NewTicker(s.lockTimeout / 2)
	defer renew.Stop()

	for {
		select {
		case <-renew.C:
			var r int
			var err error
			for {
				r, err = s.io.LockExclusive(s.oid, lockName(key),
					lockCookie, lockDesc(key), s.lockTimeout, &flags)
				if err == nil {
					break
				}
				s.log.Warn("failed to renew lock",
					zap.String("key", key), zap.Error(err))
				select {
				case <-stop:
					s.log.Debug("stop lock maintenance", zap.String("key", key))
					return nil
				case <-time.After(lockRetryInternal):
				}

			}
			if r != 0 {
				s.log.Error("failed to renew lock",
					zap.String("key", key), zap.Int("ret", r))
				return errRenewalFailed
			}
			s.log.Debug("renewed lock", zap.String("key", key))

		case <-stop:
			s.log.Debug("stop lock maintenance", zap.String("key", key))
			return nil
		}
	}
}

func (s *Storage) Unlock(key string) error {
	if s.dropLock(key) == nil {
		return errors.New("not locked by us!")
	}
	r, err := s.io.Unlock(s.oid, lockName(key), lockCookie)
	if err != nil {
		return err
	}
	if r == 0 {
		s.log.Debug("relinquished lock", zap.String("key", key))
		return nil
	}
	if r == -int(syscall.ENOENT) {
		return errors.New("not locked by us!")
	}

	return fmt.Errorf("unlock: %d", r)
}

func (s *Storage) Store(key string, value []byte) error {
	if key == "" {
		return errors.New("store: empty key")
	}
	return s.io.SetOmap(s.oid, map[string][]byte{key: value})
}

func (s *Storage) Load(key string) ([]byte, error) {
	omap, err := s.io.GetOmapValues(s.oid, "", key, 1)
	if err != nil {
		return nil, err
	}
	if v, ok := omap[key]; ok {
		return v, nil
	}

	return nil, fs.ErrNotExist
}

func (s *Storage) Delete(key string) error {
	return s.io.RmOmapKeys(s.oid, []string{key})
}

func (s *Storage) Exists(key string) bool {
	// I don't think we can implement this more efficiently on top of OMAP.
	_, err := s.Load(key)
	return err == nil
}

func (s *Storage) List(prefix string, recursive bool) ([]string, error) {
	// The certmagic.Storage interface is underspecified. Try to match
	// whatever certmagic.FileStorage does.
	// https://github.com/caddyserver/certmagic/issues/58

	// List("a/b") should not return "a/bb"
	prefix += "/"
	omap, err := s.io.GetAllOmapValues(s.oid, "", prefix, 2<<30)
	if err != nil {
		return nil, err
	}
	if len(omap) == 0 {
		return nil, fs.ErrNotExist
	}

	seen := make(map[string]bool)
	var keys []string
	for k := range omap {
		walkKey(k, len(prefix), recursive, func(k string) {
			if seen[k] {
				return
			}
			seen[k] = true
			keys = append(keys, k)
		})
	}

	return keys, nil
}

func (s *Storage) Stat(key string) (certmagic.KeyInfo, error) {
	v, err := s.Load(key)
	if err == nil {
		info := certmagic.KeyInfo{
			Key:        key,
			Size:       int64(len(v)),
			IsTerminal: true,
		}

		return info, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return certmagic.KeyInfo{}, err
	}

	// Maybe we can synthesize a non-terminal (directory) key.
	omap, err := s.io.GetAllOmapValues(s.oid, "", key+"/", 1)
	if err != nil {
		return certmagic.KeyInfo{}, err
	}
	if len(omap) == 0 {
		return certmagic.KeyInfo{}, fs.ErrNotExist
	}

	info := certmagic.KeyInfo{
		Key:        key,
		IsTerminal: false,
	}

	return info, nil
}

// Derived from:
// https://github.com/oyato/certmagic-badgerstorage/blob/0a33ad4aff89179a5be229b5005b071554faa337/storage.go#L156
// Copyright (c) 2020 oyato cloud
// License: MIT (same as this package)
// https://github.com/oyato/certmagic-badgerstorage/blob/0a33ad4aff89179a5be229b5005b071554faa337/LICENSE
func walkKey(k string, sp int, recursive bool, f func(string)) {
	if sp >= len(k) {
		return
	}
	if i := strings.IndexByte(k[sp:], '/'); i >= 0 {
		sp += i
	} else {
		sp = len(k)
	}
	f(k[:sp])
	if recursive {
		walkKey(k, sp+1, recursive, f)
	}
}
