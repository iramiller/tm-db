package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisDBNewRedisDB(t *testing.T) {
	db, err := NewRedisDB("localhost:6379", "")
	require.Nil(t, err)
	require.NotNil(t, db)
	require.NoError(t, db.Close())

	db, err = NewRedisDBWithOpts("localhost:6379", "", 13, nil)
	require.Nil(t, err)
	require.NotNil(t, db)
	require.NoError(t, db.Close())
}

func BenchmarkRedisDBRangeScans1M(b *testing.B) {
	db, err := NewRedisDBWithOpts("localhost:6379", "", 13, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanUp(db)

	assert.NoError(b, unsafeReset(db))
	benchmarkRangeScans(b, db, int64(1e6))
}

func BenchmarkRedisDBRangeScans10M(b *testing.B) {
	db, err := NewRedisDBWithOpts("localhost:6379", "", 13, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanUp(db)

	assert.NoError(b, unsafeReset(db))
	benchmarkRangeScans(b, db, int64(10e6))
}

func BenchmarkRedisDBRandomReadsWrites(b *testing.B) {
	db, err := NewRedisDBWithOpts("localhost:6379", "", 13, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanUp(db)

	assert.NoError(b, unsafeReset(db))
	benchmarkRandomReadsWrites(b, db)
}

func cleanUp(db *RedisDB) {
	db.Close()
}
