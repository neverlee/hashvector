
#include "redismodule.h"
#include <ctype.h>

int HIncrByIVCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    // open the key and make sure it is indeed a Hash and not empty
    RedisModuleKey *key =
        RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

    if ((RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) &&
            (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH)) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    size_t lvec = argc-3;
    long long vec[lvec];
    int i;
    for (i=3; i<argc; i++) {
        if (REDISMODULE_OK != RedisModule_StringToLongLong(argv[i], vec+i-3)) {
            return RedisModule_WrongArity(ctx);
        }
    }

    // get the current value of the hash element
    RedisModuleString *oldval;
    RedisModule_HashGet(key, REDISMODULE_HASH_NONE, argv[2], &oldval, NULL);
    char *poldval = NULL;
    size_t loldval = 0;
    if (oldval) {
        poldval = (char*)RedisModule_StringPtrLen(oldval, &loldval);
    }

    int lold = (loldval >> 3);
    long long *pold = (long long*)(poldval);
    if (lold >= lvec) {
        for (i=0; i<lvec; i++) {
            pold[i] += vec[i];
        }
        RedisModule_HashSet(key, REDISMODULE_HASH_NONE, argv[2], oldval, NULL);
    } else {
        for (i=0; i<lold; i++) {
            vec[i] += pold[i];
        }
        RedisModuleString *newval;
        newval = RedisModule_CreateString(ctx, (char*)vec, lvec*8);
        RedisModule_HashSet(key, REDISMODULE_HASH_NONE, argv[2], newval, NULL);
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int HIncrByFVCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    // open the key and make sure it is indeed a Hash and not empty
    RedisModuleKey *key =
        RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

    if ((RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) &&
            (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH)) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    size_t lvec = argc-3;
    double vec[lvec];
    int i;
    for (i=3; i<argc; i++) {
        if (REDISMODULE_OK != RedisModule_StringToDouble(argv[i], vec+i-3)) {
            return RedisModule_WrongArity(ctx);
        }
    }

    // get the current value of the hash element
    RedisModuleString *oldval;
    RedisModule_HashGet(key, REDISMODULE_HASH_NONE, argv[2], &oldval, NULL);
    char *poldval = NULL;
    size_t loldval = 0;
    if (oldval) {
        poldval = (char*)RedisModule_StringPtrLen(oldval, &loldval);
    }

    int lold = (loldval >> 3);
    double *pold = (double*)(poldval);
    if (lold >= lvec) {
        for (i=0; i<lvec; i++) {
            pold[i] += vec[i];
        }
        RedisModule_HashSet(key, REDISMODULE_HASH_NONE, argv[2], oldval, NULL);
    } else {
        for (i=0; i<lold; i++) {
            vec[i] += pold[i];
        }
        RedisModuleString *newval;
        newval = RedisModule_CreateString(ctx, (char*)vec, lvec*8);
        RedisModule_HashSet(key, REDISMODULE_HASH_NONE, argv[2], newval, NULL);
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int HSetIVCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    // open the key and make sure it is indeed a Hash and not empty
    RedisModuleKey *key =
        RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

    if ((RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) &&
            (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH)) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    size_t lvec = argc-3;
    long long vec[lvec];
    int i;
    for (i=3; i<argc; i++) {
        if (REDISMODULE_OK != RedisModule_StringToLongLong(argv[i], vec+i-3)) {
            return RedisModule_WrongArity(ctx);
        }
    }

    RedisModuleString *newval;
    newval = RedisModule_CreateString(ctx, (char*)vec, lvec*8);
    RedisModule_HashSet(key, REDISMODULE_HASH_NONE, argv[2], newval, NULL);
    RedisModule_FreeString(ctx, newval);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int HSetFVCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    // open the key and make sure it is indeed a Hash and not empty
    RedisModuleKey *key =
        RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

    if ((RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) &&
            (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH)) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    size_t lvec = argc-3;
    double vec[lvec];
    int i;
    for (i=3; i<argc; i++) {
        if (REDISMODULE_OK != RedisModule_StringToDouble(argv[i], vec+i-3)) {
            return RedisModule_WrongArity(ctx);
        }
    }

    RedisModuleString *newval;
    newval = RedisModule_CreateString(ctx, (char*)vec, lvec*8);
    RedisModule_HashSet(key, REDISMODULE_HASH_NONE, argv[2], newval, NULL);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}


int HGetIVCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    // open the key and make sure it is indeed a Hash and not empty
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    if ((RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) &&
            (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH)) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    // get the current value of the hash element
    RedisModuleString *val;
    RedisModule_HashGet(key, REDISMODULE_HASH_NONE, argv[2], &val, NULL);

    size_t vallen;
    const char *pval = RedisModule_StringPtrLen(val, &vallen);

    if (!val) {
        RedisModule_ReplyWithNull(ctx);
    } else if ((vallen & 0x7) != 0) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid count");
    } else {
        RedisModule_ReplyWithArray(ctx, vallen>>3);
        size_t i;
        for (i=0; i<vallen; i+=8) {
            RedisModule_ReplyWithLongLong(ctx, *((long long*)(pval+i)));
        }
    }

    return REDISMODULE_OK;
}

int HGetFVCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    // open the key and make sure it is indeed a Hash and not empty
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    if ((RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) &&
            (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH)) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    // get the current value of the hash element
    RedisModuleString *val;
    RedisModule_HashGet(key, REDISMODULE_HASH_NONE, argv[2], &val, NULL);

    size_t vallen;
    const char *pval = RedisModule_StringPtrLen(val, &vallen);

    if (!val) {
        RedisModule_ReplyWithNull(ctx);
    } else if ((vallen & 0x7) != 0) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid count");
    } else {
        RedisModule_ReplyWithArray(ctx, vallen>>3);
        size_t i;
        for (i=0; i<vallen; i+=8) {
            RedisModule_ReplyWithDouble(ctx, *((double*)(pval+i)));
        }
    }

    return REDISMODULE_OK;
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"HashVector",1,REDISMODULE_APIVER_1)
            == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "hincrbyiv", HIncrByIVCommand,
                "write fast deny-oom", 1, 1,
                -1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "hincrbyfv", HIncrByFVCommand,
                "write fast deny-oom", 1, 1,
                -1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "hsetiv", HSetIVCommand,
                "write fast deny-oom", 1, 1,
                -1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "hsetfv", HSetFVCommand,
                "write fast deny-oom", 1, 1,
                -1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "hgetiv", HGetIVCommand,
                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "hgetfv", HGetFVCommand,
                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
