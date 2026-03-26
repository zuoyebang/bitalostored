package uredis

import (
	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"math/rand"
	"reflect"
	"time"

	redigo "github.com/gomodule/redigo/redis"
)

type StoredConnConf struct {
	HostPort     string        `toml:"host_port" json:"host_port,omitempty"`
	MaxIdle      int           `toml:"max_idle" json:"max_idle"`
	MaxActive    int           `toml:"max_active" json:"max_active"`
	IdleTimeout  time.Duration `toml:"idle_timeout" json:"idle_timeout"`
	ConnLifeTime time.Duration `toml:"conn_lifetime" json:"conn_lifetime"`
	Password     string        `toml:"password" json:"password"`
	DataBase     int           `toml:"database" json:"database"`
	ConnTimeout  time.Duration `toml:"conn_timeout" json:"conn_timeout"`
	ReadTimeout  time.Duration `toml:"read_timeout" json:"read_timeout"`
	WriteTimeout time.Duration `toml:"write_timeout" json:"write_timeout"`
}

type StoredPool struct {
	Pool *redigo.Pool
}

func NewStoredPool(pool *redigo.Pool) *StoredPool {
	return &StoredPool{pool}
}

func GetStoredConfigPool(storedList []string) *redigo.Pool {
	storedConfg := &StoredConnConf{
		MaxIdle:      50,
		MaxActive:    50,
		IdleTimeout:  time.Minute,
		ConnTimeout:  100 * time.Millisecond,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
	}
	return GetPoolWithFailover(storedList, storedConfg)
}

func GetPoolWithFailover(hosts []string, conf *StoredConnConf) *redigo.Pool {
	return &redigo.Pool{
		MaxIdle:         conf.MaxIdle,
		MaxActive:       conf.MaxActive,
		IdleTimeout:     conf.IdleTimeout,
		MaxConnLifetime: conf.ConnLifeTime,
		Wait:            true,
		Dial: func() (conn redigo.Conn, e error) {
			shuffled := make([]string, len(hosts))
			copy(shuffled, hosts)
			rand.Shuffle(len(shuffled), func(i, j int) {
				shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
			})

			var lastErr error
			for _, hostport := range shuffled {
				conn, err := redigo.Dial("tcp", hostport,
					redigo.DialPassword(""),
					redigo.DialDatabase(0),
					redigo.DialConnectTimeout(conf.ConnTimeout),
					redigo.DialReadTimeout(conf.ReadTimeout),
					redigo.DialWriteTimeout(conf.WriteTimeout),
				)
				if err != nil {
					log.Warn("get_redis_conn_fail: ", err)
					lastErr = err
					continue
				}
				log.Infof("connect via stored-config %s", hostport)
				return conn, nil
			}
			return nil, lastErr
		},
		TestOnBorrow: func(c redigo.Conn, t time.Time) error {
			if time.Since(t) < 30*time.Second {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func GetPool(conf *StoredConnConf) *redigo.Pool {
	return &redigo.Pool{
		MaxIdle:         conf.MaxIdle,
		MaxActive:       conf.MaxActive,
		IdleTimeout:     conf.IdleTimeout,
		MaxConnLifetime: conf.ConnLifeTime,
		Wait:            true,
		Dial: func() (conn redigo.Conn, e error) {
			conn, err := redigo.Dial("tcp", conf.HostPort,
				redigo.DialPassword(""),
				redigo.DialDatabase(0),
				redigo.DialConnectTimeout(conf.ConnTimeout),
				redigo.DialReadTimeout(conf.ReadTimeout),
				redigo.DialWriteTimeout(conf.WriteTimeout),
			)
			if err != nil {
				log.Warn("get_redis_conn_fail: ", err)
				return nil, err
			}
			return conn, nil
		},
		TestOnBorrow: func(c redigo.Conn, t time.Time) error {
			if time.Since(t) < 30*time.Second {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func (sp *StoredPool) HGet(key string, field string) ([]byte, error) {
	client := sp.Pool.Get()
	defer client.Close()
	if res, err := redigo.Bytes(client.Do("HGET", key, field)); err == redigo.ErrNil {
		return nil, nil
	} else {
		return res, err
	}
}

func (sp *StoredPool) HMSet(key string, fvmap map[string]interface{}) error {
	client := sp.Pool.Get()
	defer client.Close()
	args := PackArgs(key, fvmap)
	_, err := client.Do("HMSET", args...)
	return err
}

func (sp *StoredPool) Del(keys ...interface{}) (int64, error) {
	client := sp.Pool.Get()
	defer client.Close()
	args := PackArgs(keys)
	return redigo.Int64(client.Do("DEL", args...))
}

func (sp *StoredPool) HGetall(key string) ([][]byte, error) {
	client := sp.Pool.Get()
	defer client.Close()
	if res, err := redigo.ByteSlices(client.Do("HGETALL", key)); err == redigo.ErrNil {
		return nil, nil
	} else {
		return res, err
	}
}

func PackArgs(items ...interface{}) (args []interface{}) {
	for _, item := range items {
		v := reflect.ValueOf(item)
		switch v.Kind() {
		case reflect.Slice:
			if v.IsNil() {
				continue
			}
			for i := 0; i < v.Len(); i++ {
				args = append(args, v.Index(i).Interface())
			}
		case reflect.Map:
			if v.IsNil() {
				continue
			}
			for _, key := range v.MapKeys() {
				args = append(args, key.Interface(), v.MapIndex(key).Interface())
			}
		default:
			args = append(args, v.Interface())
		}
	}
	return args
}
