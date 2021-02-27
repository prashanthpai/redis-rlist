# redis-rlist

A Redis module that adds rate limiting over Redis lists. Currently, rate limit
is applied only on the list from which items are popped.

## Install

```sh
$ make build
$ redis-server --loadmodule ~/path/to/rlist.so
```

## Usage

* **key**: A normal redis list structure.
* **interval**: Rate limit interval in milliseconds.

```
RL.SETPOPINTERVAL key interval [key interval ...]
RL.LPOP key
RL.RPOP key
RL.RPOPLPUSH source destination
```

**Example:**

```
127.0.0.1:6379> LPUSH mylist 1 2 4 5 6 7 8 9
(integer) 8
127.0.0.1:6379> RL.SETPOPINTERVAL mylist 3000
(integer) 0
127.0.0.1:6379> RL.LPOP mylist
"9"
127.0.0.1:6379> RL.LPOP mylist
(nil)
127.0.0.1:6379> RL.LPOP mylist
(nil)
127.0.0.1:6379> RL.LPOP mylist
"8"
```

## TODO

* Support applying ratelimit to destination list.
* LMOVE (redis >= 6.2.0).
* Blocking versions (BLPOP, BRPOP, BRPOPLPUSH, BLMOVE) depending on whether it's worth it (timers etc).
* Rewrite in rust (for learning rust).

## Redis module development reference

Just some bookmarks for development.

* [Redis Modules: an introduction to the API](https://redis.io/topics/modules-intro)
* [Modules API reference](https://redis.io/topics/modules-api-ref)
* [Native types in Redis modules](https://redis.io/topics/modules-native-types)
* [Blocking commands in Redis modules](https://redis.io/topics/modules-blocking-ops)
* [RedisModulesSDK](https://github.com/RedisLabs/RedisModulesSDK)
