package com.yinrs.cache;

import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CacheManager {

    void init();

    void destroy();

    void del(String key);

    long hincrBy(String key, String field, long value);

    void set(String key, String value);

    void set(String key, Object value);

    void set(String key, String value, int expireTime);

    void set(String key, Object value, int expireTime);

    <T> T get(String key, Class<T> tClass);

    void hset(String key, String field, String value);

    void hset(String key, String field, Object value);

    <T> T hget(String key, String field, Class<T> tClass);

    void hdel(String key, String... field);

    <T> Map<String, T> hgetAll(String key, Class<T> clazz);

    void zAdd(String key, String value);

    void zadd(String key, double score, String member);

    Long zadd(String key, Map<String, Double> scoreMembers);

    Double zincrby(String key, double score, String member);

    Double zscore(String key, String member);

    Long zCard(String key);

    Long zcard(String key);

    Long zcount(String key, double min, double max);

    void zRem(String key, String value);

    Long zrem(String key, String... member);

    Long zremrangeByRank(String key, long start, long end);

    Long zremrangeByScore(String key, double start, double end);

    Long zrank(String key, String member);

    Long zrevrank(String key, String member);

    Set<Tuple> zrangeWithScores(String key, long start, long end);

    Set<Tuple> zrevrangeWithScores(String key, long start, long end);

    <T> List<T> zrange(String key, int start, int end, Class<T> clazz);

    void lpush(String key, String value);

    void lRem(String key, String value);

    void sAdd(String key, String value);

    void sRem(String key, String value);

    boolean sismember(String key, String value);

    Boolean sismember1(String key, String member);

    <T> List<T> sScan(String key, Class<T> clazz, int start);

    List<String> sScan(String key, int start);

    Long sCard(String key);

    <T> List<T> lrange(String key, int start, int end, Class<T> clazz);
}

