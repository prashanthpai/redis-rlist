#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/time.h>

#include "redismodule.h"

static RedisModuleString* pop_intervals_key = NULL;
static RedisModuleString* push_intervals_key = NULL;
static RedisModuleString* last_pop_times_key = NULL;
static RedisModuleString* last_push_times_key = NULL;

int RLSetRL_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
	RedisModule_AutoMemory(ctx);

	if (argc < 3 || (argc % 2 != 1))
		return RedisModule_WrongArity(ctx);

	size_t len;
	RedisModuleKey* key = NULL;
	if (strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.SETPOPINTERVAL", 17) == 0) {
		key = RedisModule_OpenKey(ctx, pop_intervals_key, REDISMODULE_READ | REDISMODULE_WRITE);
	} else if (strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.SETPUSHINTERVAL", 18) == 0) {
		key = RedisModule_OpenKey(ctx, push_intervals_key, REDISMODULE_READ | REDISMODULE_WRITE);
	} else {
		return RedisModule_ReplyWithError(ctx, "ERR: invalid command and/or args");
	}

	int type = RedisModule_KeyType(key);
	if (type != REDISMODULE_KEYTYPE_HASH && type != REDISMODULE_KEYTYPE_EMPTY) {
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
	}

	// validate that intervals are valid
	long long interval;
	for (int i = 2; i < argc; i = i + 2) {
		int ret = RedisModule_StringToLongLong(argv[i], &interval);
		if (ret != REDISMODULE_OK)
			return RedisModule_ReplyWithError(ctx, "ERR: one or more intervals is invalid");

		if (interval < 0)
			return RedisModule_ReplyWithError(ctx, "ERR: one or more intervals is invalid");
	}

	// HSET in a loop because of https://github.com/redis/redis/issues/7860
	int count = 0;
	for (int i = 1; i + 1 < argc; i = i + 2) {
		int ret = RedisModule_HashSet(key, REDISMODULE_HASH_NONE, argv[i], argv[i + 1], NULL);
		count += ret;
	}

	return RedisModule_ReplyWithLongLong(ctx, count);
}

int RL_LPopRPushCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
	RedisModule_AutoMemory(ctx);

	if (argc != 3)
		return RedisModule_WrongArity(ctx);

	RedisModuleKey* src_key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
	RedisModuleKey* dst_key = RedisModule_OpenKey(ctx, argv[2], REDISMODULE_READ | REDISMODULE_WRITE);

	int src_type = RedisModule_KeyType(src_key);
	int dst_type = RedisModule_KeyType(dst_key);

	if ((src_type != REDISMODULE_KEYTYPE_LIST && src_type != REDISMODULE_KEYTYPE_EMPTY) ||
	    (dst_type != REDISMODULE_KEYTYPE_LIST && dst_type != REDISMODULE_KEYTYPE_EMPTY)) {
		RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
		return REDISMODULE_ERR;
	}

	RedisModuleString* val = RedisModule_ListPop(src_key, REDISMODULE_LIST_HEAD);
	if (val == NULL)
		return RedisModule_ReplyWithNull(ctx);

	RedisModule_ListPush(dst_key, REDISMODULE_LIST_TAIL, val);

	return RedisModule_ReplyWithString(ctx, val);
}

static int dispatch_move(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
	size_t len;
	RedisModuleCallReply* reply = NULL;

	if (strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.LPOP", 7) == 0 && argc == 2) {
		reply = RedisModule_Call(ctx, "LPOP", "s", argv[1]);
	} else if (strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.RPOP", 7) == 0 && argc == 2) {
		reply = RedisModule_Call(ctx, "RPOP", "s", argv[1]);
	} else if (strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.LPUSH", 8) == 0 && argc == 3) {
		reply = RedisModule_Call(ctx, "LPUSH", "ss", argv[1], argv[2]);
	} else if (strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.RPUSH", 8) == 0 && argc == 3) {
		reply = RedisModule_Call(ctx, "RPUSH", "ss", argv[1], argv[2]);
	} else if (strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.RPOPLPUSH", 12) == 0 && argc == 3) {
		reply = RedisModule_Call(ctx, "RPOPLPUSH", "ss", argv[1], argv[2]);
	} else if (strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.LPOPRPUSH", 12) == 0 && argc == 3) {
		return RL_LPopRPushCommand(ctx, argv, argc);
	} else {
		return RedisModule_ReplyWithError(ctx, "ERR: invalid command and/or args");
	}

	return RedisModule_ReplyWithCallReply(ctx, reply);
}

static int _get_long_long_from_hash(RedisModuleCtx* ctx, RedisModuleString* hash, RedisModuleString* field,
				    long long** result) {
	if (result == NULL)
		return REDISMODULE_ERR;

	RedisModuleKey* key = RedisModule_OpenKey(ctx, hash, REDISMODULE_READ);
	if (key == NULL) {
		// hash does not exist
		*result = NULL;
		return REDISMODULE_OK;
	}

	RedisModuleString* val_str = NULL;
	if (RedisModule_HashGet(key, REDISMODULE_HASH_NONE, field, &val_str, NULL) != REDISMODULE_OK) {
		// RedisModule_HashGet returns REDISMODULE_OK on success and
		// REDISMODULE_ERR if the key is not a hash.
		return REDISMODULE_ERR;
	}

	if (val_str == NULL) {
		// hash exists but field does not exist
		*result = NULL;
		return REDISMODULE_OK;
	}

	long long val;
	if (RedisModule_StringToLongLong(val_str, &val) != REDISMODULE_OK)
		return REDISMODULE_ERR;

	*result = &val;
	return REDISMODULE_OK;
}

static long long get_long_long_from_hash(RedisModuleCtx* ctx, RedisModuleString* hash, RedisModuleString* field) {
	long long result = 0;

	long long* val_ptr = NULL;
	if (_get_long_long_from_hash(ctx, hash, field, &val_ptr) != REDISMODULE_OK)
		return -1;

	if (val_ptr != NULL)
		result = *val_ptr;

	return result;
}

int RL_PopOrPush_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
	RedisModule_AutoMemory(ctx);

	if (argc != 2 && argc != 3)
		return RedisModule_WrongArity(ctx);

	size_t len;
	RedisModuleString* intervals_key = NULL;
	RedisModuleString* ts_key = NULL;
	if ((strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.LPOP", 7) == 0 ||
	     strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.RPOP", 7) == 0) &&
	    argc == 2) {
		intervals_key = pop_intervals_key;
		ts_key = last_pop_times_key;
	} else if ((strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.LPUSH", 8) == 0 ||
		    strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.RPUSH", 8) == 0) &&
		   argc == 3) {
		intervals_key = push_intervals_key;
		ts_key = last_push_times_key;
	} else {
		return RedisModule_ReplyWithError(ctx, "ERR: invalid command and/or args");
	}

	long long interval = get_long_long_from_hash(ctx, intervals_key, argv[1]);
	if (interval < 0)
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

	if (interval == 0)
		return dispatch_move(ctx, argv, argc);

	long long last_timestamp = get_long_long_from_hash(ctx, ts_key, argv[1]);
	if (last_timestamp < 0)
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

	struct timeval tv;
	gettimeofday(&tv, NULL);
	long long ms_epoch = (long long)(tv.tv_sec) * 1000 + (long long)(tv.tv_usec) / 1000;

	if (ms_epoch >= (last_timestamp + interval)) {
		RedisModuleKey* key = RedisModule_OpenKey(ctx, ts_key, REDISMODULE_WRITE);
		RedisModule_HashSet(key, REDISMODULE_HASH_NONE, argv[1],
				    RedisModule_CreateStringFromLongLong(ctx, ms_epoch), NULL);
		return dispatch_move(ctx, argv, argc);
	}

	return RedisModule_ReplyWithNull(ctx);
}

int RL_PopAndPush_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
	RedisModule_AutoMemory(ctx);

	if (argc != 3)
		return RedisModule_WrongArity(ctx);

	long long pop_interval = get_long_long_from_hash(ctx, pop_intervals_key, argv[1]);
	if (pop_interval < 0)
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

	long long push_interval = get_long_long_from_hash(ctx, push_intervals_key, argv[2]);
	if (push_interval < 0)
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

	if (pop_interval == 0 && push_interval == 0)
		return dispatch_move(ctx, argv, argc);

	long long last_pop_timestamp = get_long_long_from_hash(ctx, last_pop_times_key, argv[1]);
	if (last_pop_timestamp < 0)
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

	long long last_push_timestamp = get_long_long_from_hash(ctx, last_push_times_key, argv[2]);
	if (last_push_timestamp < 0)
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

	struct timeval tv;
	gettimeofday(&tv, NULL);
	long long ms_epoch = (long long)(tv.tv_sec) * 1000 + (long long)(tv.tv_usec) / 1000;

	if (ms_epoch >= (last_pop_timestamp + pop_interval) && ms_epoch >= (last_push_timestamp + push_interval)) {
		RedisModuleKey* key_pop = RedisModule_OpenKey(ctx, last_pop_times_key, REDISMODULE_WRITE);
		RedisModuleKey* key_push = RedisModule_OpenKey(ctx, last_push_times_key, REDISMODULE_WRITE);
		RedisModuleString* ts = RedisModule_CreateStringFromLongLong(ctx, ms_epoch);
		RedisModule_HashSet(key_pop, REDISMODULE_HASH_NONE, argv[1], ts, NULL);
		RedisModule_HashSet(key_push, REDISMODULE_HASH_NONE, argv[2], ts, NULL);
		return dispatch_move(ctx, argv, argc);
	}

	return RedisModule_ReplyWithNull(ctx);
}

int RedisModule_OnLoad(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
	REDISMODULE_NOT_USED(argv);
	REDISMODULE_NOT_USED(argc);

	if (RedisModule_Init(ctx, "RL", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "RL.SETPOPINTERVAL", RLSetRL_RedisCommand, "write fast deny-oom", 1, -1,
				      2) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "RL.SETPUSHINTERVAL", RLSetRL_RedisCommand, "write fast deny-oom", 1, -1,
				      2) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "RL.LPOP", RL_PopOrPush_RedisCommand, "write fast deny-oom", 1, 1, 1) ==
	    REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "RL.RPOP", RL_PopOrPush_RedisCommand, "write fast deny-oom", 1, 1, 1) ==
	    REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "RL.LPUSH", RL_PopOrPush_RedisCommand, "write fast deny-oom", 1, 1, 1) ==
	    REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "RL.RPUSH", RL_PopOrPush_RedisCommand, "write fast deny-oom", 1, 1, 1) ==
	    REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "RL.RPOPLPUSH", RL_PopAndPush_RedisCommand, "write fast deny-oom", 1, 2,
				      1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "RL.LPOPRPUSH", RL_PopAndPush_RedisCommand, "write fast deny-oom", 1, 2,
				      1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	pop_intervals_key = RedisModule_CreateString(ctx, "rl::popintervals", 16);
	if (pop_intervals_key == NULL)
		return REDISMODULE_ERR;

	push_intervals_key = RedisModule_CreateString(ctx, "rl::pushintervals", 17);
	if (push_intervals_key == NULL)
		return REDISMODULE_ERR;

	last_pop_times_key = RedisModule_CreateString(ctx, "rl::lastpoptimes", 16);
	if (last_pop_times_key == NULL)
		return REDISMODULE_ERR;

	last_push_times_key = RedisModule_CreateString(ctx, "rl::lastpushtimes", 17);
	if (last_push_times_key == NULL)
		return REDISMODULE_ERR;

	return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx* ctx) {
	if (pop_intervals_key != NULL)
		RedisModule_FreeString(ctx, pop_intervals_key);

	if (push_intervals_key != NULL)
		RedisModule_FreeString(ctx, push_intervals_key);

	if (last_pop_times_key != NULL)
		RedisModule_FreeString(ctx, last_pop_times_key);

	if (last_push_times_key != NULL)
		RedisModule_FreeString(ctx, last_push_times_key);

	return REDISMODULE_OK;
}
