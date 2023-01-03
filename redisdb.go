package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v9"
)

//
// Creates a REDIS database for storage of the tm-db key value data.
//
// Maintains a sorted set for all the keys to support iteration in order
// according to the iterator interface spec.
//

const redisKeyIndex = "__index__"

func init() {
	dbCreator := func(host string, pwd string) (DB, error) {
		return NewRedisDB(host, pwd)
	}
	registerDBCreator(RedisDBBackend, dbCreator, false)
}

type RedisDB struct {
	db *redis.Client
}

var _ DB = (*RedisDB)(nil)

var ctx = context.Background()

func NewRedisDB(host string, pwd string) (*RedisDB, error) {
	return NewRedisDBWithOpts(host, pwd, 0, nil)
}

func NewRedisDBWithOpts(host string, pwd string, dbNum int, o *redis.Options) (*RedisDB, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379", // "localhost:6379",
		Password:     pwd,              // "" == none set
		DB:           dbNum,            // use default DB 0 (zero)
		WriteTimeout: time.Minute,
		ReadTimeout:  time.Minute,
	})
	database := &RedisDB{
		db: rdb,
	}
	return database, nil
}

// Get implements DB.
func (db *RedisDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	res, err := db.db.Get(ctx, string(key)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return []byte(res), nil
}

// Has implements DB.
func (db *RedisDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}
	bytes, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return bytes != nil, nil
}

// Set implements DB.
func (db *RedisDB) Set(key []byte, value []byte) error {
	return db.SetSync(key, value)
}

// Set implements DB.
func (db *RedisDB) SetSync(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}

	_, err := db.db.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, string(key), string(value), redis.KeepTTL)
		pipe.ZAdd(ctx, redisKeyIndex, redis.Z{Score: 0, Member: string(key)})
		return nil
	})

	return err
}

// Set implements DB.
func (db *RedisDB) Delete(key []byte) error {
	return db.DeleteSync(key)
}

// Set implements DB.
func (db *RedisDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	_, err := db.db.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, string(key))
		pipe.ZRem(ctx, redisKeyIndex, string(key))
		return nil
	})

	return err
}

func (db *RedisDB) DB() *redis.Client {
	return db.db
}

// Close implements DB.
func (db *RedisDB) Close() error {
	return db.db.Close()
}

// Print implements DB.
func (db *RedisDB) Print() error {
	res, err := db.db.Info(ctx, "stats").Result()
	fmt.Printf("%v\n", res)
	iter := db.db.ZScan(ctx, redisKeyIndex, 0, "prefix:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		if val, err := db.db.Get(ctx, key).Result(); err != nil {
			panic(err)
		} else {
			fmt.Printf("[%X]:\t[%X]\n", key, val)
		}
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}

	return err
}

// Stats implements DB.
func (db *RedisDB) Stats() map[string]string {
	stats := make(map[string]string)
	res, err := db.db.Info(ctx, "stats").Result()
	if err != nil {
		panic(err)
	}
	entries := strings.Split(res, "\r\n")
	for _, e := range entries {
		if strings.HasPrefix(e, "#") {
			continue
		}
		entry := strings.Split(e, ":")
		if len(entry) > 1 {
			stats[entry[0]] = strings.Join(entry[1:], ":")
		} else {
			stats[e] = "n/a"
		}
	}
	return stats
}

// NewBatch implements DB.
func (db *RedisDB) NewBatch() Batch {
	return newRedisDBBatch(ctx, db)
}

// Iterator implements DB.
func (db *RedisDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newRedisDBIterator(ctx, *db.db, start, end, false)
}

// ReverseIterator implements DB.
func (db *RedisDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newRedisDBIterator(ctx, *db.db, start, end, true)
}

// Unsafe reset all for a database.
func unsafeReset(db *RedisDB) error {
	_, err := db.DB().Conn().FlushDB(ctx).Result()
	return err
}
