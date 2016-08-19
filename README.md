# kube-etcd-migrate
Data migration tool from etcd2 to etcd3


## How to run

Assumed you have "etcd" repo in $GOPATH.

Build it in root dir:
```
go build .
```

Migrate data dir:
```
./kube-etcd-migrate --data-dir=$datadir --timeout=3600
```
This will migrate all v2 keys into v3 keys.
For ttl keys in v2, they will be attached to a new lease with given timeout as initial TTL.