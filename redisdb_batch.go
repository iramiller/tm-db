package db

import (
	"context"

	"github.com/go-redis/redis/v9"
)

type redisDBBatch struct {
	ctx   context.Context
	db    *RedisDB
	batch redis.Pipeliner
}

var _ Batch = (*redisDBBatch)(nil)

func newRedisDBBatch(ctx context.Context, db *RedisDB) *redisDBBatch {
	return &redisDBBatch{
		ctx:   ctx,
		db:    db,
		batch: db.db.TxPipeline(),
	}
}

// Set implements Batch.
func (b *redisDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	b.batch.ZAdd(b.ctx, redisKeyIndex, redis.Z{Score: 0, Member: string(key)})
	b.batch.Set(b.ctx, string(key), string(value), redis.KeepTTL)
	return nil
}

// Delete implements Batch.
func (b *redisDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	b.batch.ZRem(b.ctx, redisKeyIndex, redis.Z{Score: 0, Member: string(key)})
	b.batch.Del(b.ctx, string(key))
	return nil
}

// Write implements Batch.
func (b *redisDBBatch) Write() error {
	return b.write(false)
}

// WriteSync implements Batch.
func (b *redisDBBatch) WriteSync() error {
	return b.write(true)
}

func (b *redisDBBatch) write(sync bool) error {
	if b.batch == nil {
		return errBatchClosed
	}
	_, err := b.batch.Exec(b.ctx)
	if err != nil {
		return err
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	return b.Close()
}

// Close implements Batch.
func (b *redisDBBatch) Close() error {
	b.batch = nil
	return nil
}
