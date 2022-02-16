module github.com/hdacloud/certmagic-rados-storage

go 1.16

require (
	github.com/caddyserver/certmagic v0.15.3
	github.com/ceph/go-ceph v0.13.0
	github.com/oyato/certmagic-storage-tests v0.0.0-20200322161024-236af4dad398
	go.uber.org/zap v1.21.0
)

replace github.com/oyato/certmagic-storage-tests => github.com/hdacloud/certmagic-storage-tests v0.0.0-20220214105535-cf0753a814a3
