#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/time.h>

#include "redismodule.h"

static RedisModuleString* popintervals_key = NULL;
static RedisModuleString* last_dequeue_times_key = NULL;

int RLSetRL_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
	RedisModule_AutoMemory(ctx);

	if (argc < 3 || (argc % 2 != 1))
		return RedisModule_WrongArity(ctx);

	RedisModuleKey* key = RedisModule_OpenKey(ctx, popintervals_key, REDISMODULE_READ | REDISMODULE_WRITE);

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

static int dispatch_move(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
	size_t len;
	RedisModuleCallReply* reply = NULL;

	if (strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.LPOP", 8) == 0 && argc == 2) {
		reply = RedisModule_Call(ctx, "LPOP", "s", argv[1]);
	} else if (strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.RPOP", 8) == 0 && argc == 2) {
		reply = RedisModule_Call(ctx, "RPOP", "s", argv[1]);
	} else if (strncasecmp(RedisModule_StringPtrLen(argv[0], &len), "RL.RPOPLPUSH", 13) == 0 && argc == 3) {
		reply = RedisModule_Call(ctx, "RPOPLPUSH", "ss", argv[1], argv[2]);
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

int RL_Nonblock_Move_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
	RedisModule_AutoMemory(ctx);

	if (argc != 2 && argc != 3) {
		// RPOP has 2, RPOPLPUSH has 3
		return RedisModule_WrongArity(ctx);
	}

	long long interval = get_long_long_from_hash(ctx, popintervals_key, argv[1]);
	if (interval < 0)
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

	if (interval == 0)
		return dispatch_move(ctx, argv, argc);

	long long last_dequeue_time = get_long_long_from_hash(ctx, last_dequeue_times_key, argv[1]);
	if (last_dequeue_time < 0)
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

	struct timeval tv;
	gettimeofday(&tv, NULL);
	long long ms_epoch = (long long)(tv.tv_sec) * 1000 + (long long)(tv.tv_usec) / 1000;

	if (ms_epoch >= (last_dequeue_time + interval)) {
		RedisModuleKey* dqt_key = RedisModule_OpenKey(ctx, last_dequeue_times_key, REDISMODULE_WRITE);
		RedisModule_HashSet(dqt_key, REDISMODULE_HASH_NONE, argv[1],
				    RedisModule_CreateStringFromLongLong(ctx, ms_epoch), NULL);
		return dispatch_move(ctx, argv, argc);
	}

	return RedisModule_ReplyWithNullArray(ctx);
}

int RedisModule_OnLoad(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
	REDISMODULE_NOT_USED(argv);
	REDISMODULE_NOT_USED(argc);

	if (RedisModule_Init(ctx, "RL", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "RL.SETPOPINTERVAL", RLSetRL_RedisCommand, "write fast deny-oom", 1, -1,
				      2) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "RL.LPOP", RL_Nonblock_Move_RedisCommand, "write fast deny-oom", 1, 1, 1) ==
	    REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "RL.RPOP", RL_Nonblock_Move_RedisCommand, "write fast deny-oom", 1, 1, 1) ==
	    REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "RL.RPOPLPUSH", RL_Nonblock_Move_RedisCommand, "write fast deny-oom", 1, 2,
				      1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	popintervals_key = RedisModule_CreateString(ctx, "rl::popintervals", 16);
	if (popintervals_key == NULL)
		return REDISMODULE_ERR;

	last_dequeue_times_key = RedisModule_CreateString(ctx, "rl::lastdequeuetimes", 20);
	if (last_dequeue_times_key == NULL)
		return REDISMODULE_ERR;

	return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx* ctx) {
	if (popintervals_key != NULL)
		RedisModule_FreeString(ctx, popintervals_key);

	if (last_dequeue_times_key != NULL)
		RedisModule_FreeString(ctx, last_dequeue_times_key);

	return REDISMODULE_OK;
}
