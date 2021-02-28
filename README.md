# redis-rlist

Rate limited operations (pop, push) for redis lists.

This redis module allows applying rate limiting to normal Redis lists.
Rate limits can be applied for both pops from a list and pushes to a list.

This was a weekend project for learning to build redis modules.

## Install

```sh
$ make build
$ redis-server --loadmodule rlist.so
```

This module has been developed and tested with redis `6.0.9`. However, there's
no reason for this to not work with other redis versions.

## Usage

* **key**: A normal redis list structure.
* **interval**: Rate limit interval in milliseconds.

Available commands:

```
RL.SETPOPINTERVAL key interval [key interval ...]
RL.SETPUSHINTERVAL key interval [key interval ...]
RL.LPOP key
RL.RPOP key
RL.LPUSH key element
RL.RPUSH key element
RL.RPOPLPUSH source destination
RL.LPOPRPUSH source destination
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

* Add tests.
* Blocking versions (BLPOP, BRPOP, BRPOPLPUSH, BLMOVE) depending on whether it's worth it (timers etc).
* Rewrite in rust (for learning rust).
* LMOVE (redis >= 6.2.0).

## Redis module development reference

Just some bookmarks for development.

* [Redis Modules: an introduction to the API](https://redis.io/topics/modules-intro)
* [Modules API reference](https://redis.io/topics/modules-api-ref)
* [Native types in Redis modules](https://redis.io/topics/modules-native-types)
* [Blocking commands in Redis modules](https://redis.io/topics/modules-blocking-ops)
* [RedisModulesSDK](https://github.com/RedisLabs/RedisModulesSDK)
