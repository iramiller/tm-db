package db

import (
	"context"

	"github.com/go-redis/redis/v9"
)

const (
	// limit to use for buffering
	dbBufferLimit = 10000
)

type kvItem struct {
	key []byte
	val []byte
}

type redisDBIterator struct {
	client redis.Client
	ctx    context.Context
	cancel context.CancelFunc

	start []byte
	end   []byte

	buff <-chan *kvItem
	item *kvItem

	isInvalid bool
}

var _ Iterator = (*redisDBIterator)(nil)

func newRedisDBIterator(ctx context.Context, db redis.Client, start, end []byte, isReverse bool) (*redisDBIterator, error) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *kvItem, dbBufferLimit)

	iter := &redisDBIterator{
		client: db,
		ctx:    ctx,
		cancel: cancel,

		start: start,
		end:   end,

		buff:      ch,
		isInvalid: false,
	}

	var keySetFn func(offset int64) *redis.StringSliceCmd
	var lowKey, highKey string

	if end == nil {
		highKey = "+"
	} else {
		highKey = "(" + string(end[:])
	}
	if start == nil {
		lowKey = "-"
	} else {
		lowKey = "[" + string(start[:])
	}

	if isReverse {
		keySetFn = func(offset int64) *redis.StringSliceCmd {
			//return db.ZRevRange(ctx, redisKeyIndex, maximum(upperBound-offset, 0), minimum(lowerBound-offset-dbBufferLimit, lowerBound))
			return db.ZRevRangeByLex(ctx, redisKeyIndex, &redis.ZRangeBy{Max: highKey, Min: lowKey, Offset: offset, Count: dbBufferLimit})
		}
	} else {
		keySetFn = func(offset int64) *redis.StringSliceCmd {
			//return db.ZRange(ctx, redisKeyIndex, lowerBound+offset, minimum(lowerBound+offset+dbBufferLimit, upperBound))
			return db.ZRangeByLex(ctx, redisKeyIndex, &redis.ZRangeBy{Min: lowKey, Max: highKey, Offset: offset, Count: dbBufferLimit})
		}
	}

	go func() {
		defer close(ch)
		offset := int64(0)
		for {
			keys, err := keySetFn(offset).Result()
			if err != nil || len(keys) == 0 {
				cancel()
				return
			}
			values, err := db.MGet(ctx, keys...).Result()
			if err != nil {
				cancel()
				return
			}

			for i := range keys {
				item := &kvItem{key: []byte(keys[i]), val: []byte(values[i].(string))}
				select {
				case <-ctx.Done():
					return
				case ch <- item:
					offset += 1
				}
			}
		}
	}()

	// prime the iterator with the first value, if any
	if item, ok := <-ch; ok {
		iter.item = item
	}

	return iter, nil
}

// Domain implements Iterator.
func (itr *redisDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Valid implements Iterator.
func (itr *redisDBIterator) Valid() bool {
	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// If source errors, invalid.
	if err := itr.Error(); err != nil {
		itr.isInvalid = true
		return false
	}

	// No no item, not valid.
	return itr.item != nil
}

// Key implements Iterator.
func (itr *redisDBIterator) Key() []byte {
	itr.assertIsValid()
	return itr.item.key
}

// Value implements Iterator.
func (itr *redisDBIterator) Value() []byte {
	itr.assertIsValid()
	return itr.item.val
}

// Next implements Iterator.
func (itr *redisDBIterator) Next() {
	itr.assertIsValid()
	item, ok := <-itr.buff
	switch {
	case ok:
		itr.item = item
	default:
		itr.item = nil
	}
}

// Error implements Iterator.
func (itr *redisDBIterator) Error() error {
	return nil
}

// Close implements Iterator.
func (itr *redisDBIterator) Close() error {
	itr.cancel()
	for range itr.buff { // drain channel
	}
	itr.item = nil
	return nil
}

func (itr redisDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}
