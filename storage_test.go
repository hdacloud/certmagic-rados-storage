package certmagicrados

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	tests "github.com/oyato/certmagic-storage-tests"

	"github.com/ceph/go-ceph/rados"
	"go.uber.org/zap"
)

func TestLock(t *testing.T) {
	ctx := context.Background()
	s, closeFn := mustMakeStorage(t)
	t.Cleanup(closeFn)

	t.Run("LockUnlock", func(t *testing.T) {
		const N = 10
		for i := 0; i < N; i++ {
			if err := s.Lock(ctx, t.Name()); err != nil {
				t.Fatal(err)
			}
			if err := s.Unlock(t.Name()); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("LockDistinctKeys", func(t *testing.T) {
		// We should be able to acquire locks for multiple distinct
		// keys.
		for i := 0; i < 2; i++ {
			key := t.Name() + strconv.Itoa(i)
			if err := s.Lock(ctx, key); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 2; i++ {
			key := t.Name() + strconv.Itoa(i)
			if err := s.Unlock(key); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("LockNotReentrant", func(t *testing.T) {
		// Locking the same key twice should not be possible though.
		key := t.Name()

		if err := s.Lock(ctx, key); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		if err := s.Lock(ctx, key); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got error %v, want DeadlineExceeded", err)
		}
		if err := s.Unlock(key); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBadgerSuite(t *testing.T) {
	// Re-use certmagic-badgerstorage's test suite
	// https://github.com/oyato/certmagic-storage-tests

	ctx := context.Background()
	s, closeFn := mustMakeStorage(t)
	t.Cleanup(closeFn)

	tests.NewTestSuite(s).Run(t)
	_ = ctx

}

func dialCeph() (c *rados.Conn, err error) {
	if s := os.Getenv("CEPH_USER"); s != "" {
		c, err = rados.NewConnWithUser(s)
	} else {
		c, err = rados.NewConn()
	}
	if err != nil {
		return nil, err
	}
	if err := c.ReadDefaultConfigFile(); err != nil {
		return nil, err
	}
	if err := c.Connect(); err != nil {
		return nil, err
	}

	return c, nil
}

func mustMakeStorage(t *testing.T) (*Storage, func()) {
	pool := os.Getenv("CEPH_POOL")
	if pool == "" {
		t.Skip("skipping test, set CEPH_POOL to run")
	}
	c, err := dialCeph()
	if err != nil {
		t.Fatal(err)
	}
	ioctx, err := c.OpenIOContext(pool)
	if err != nil {
		t.Fatal(err)
	}
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}

	s := NewStorage(ioctx, t.Name(), WithLogger(logger))

	close := func() {
		s.io = nil
		if err := ioctx.Delete(t.Name()); err != nil {
			t.Logf("cleanup RADOS object failed: %v (oid=%q)",
				err, t.Name())
		}
		ioctx.Destroy()
		c.Shutdown()
	}
	return s, close
}
