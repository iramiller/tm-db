module github.com/tendermint/tm-db

go 1.17

require (
	github.com/cosmos/gorocksdb v1.2.0
	github.com/dgraph-io/badger/v2 v2.2007.2
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.4.3 // indirect
	github.com/google/btree v1.0.0
	github.com/jmhodges/levigo v1.0.0
	github.com/stretchr/testify v1.8.1
	github.com/syndtr/goleveldb v1.0.1-0.20200815110645-5c35d600f0ca
	go.etcd.io/bbolt v1.3.6
	google.golang.org/grpc v1.44.0
)

require github.com/go-redis/redis/v9 v9.0.0-rc.2

require (
	github.com/DataDog/zstd v1.4.1 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.0.3-0.20200630154024-f66de99634de // indirect
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/pkg/errors v0.8.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.2.0 // indirect
	golang.org/x/sys v0.2.0 // indirect
	golang.org/x/text v0.4.0 // indirect
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013 // indirect
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Breaking changes were released with the wrong tag (use v0.6.6 or later).
retract v0.6.5
