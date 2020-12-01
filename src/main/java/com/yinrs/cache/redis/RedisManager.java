package com.yinrs.cache.redis;

import com.yinrs.cache.CacheManager;
import com.yinrs.cache.redis.connection.RedisConnectionFactory;
import com.yinrs.config.CC;
import com.yinrs.tools.Jsons;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.NumberUtils;
import redis.clients.jedis.*;

import java.util.*;

public final class RedisManager implements CacheManager {
    private static final Logger log = LoggerFactory.getLogger(RedisManager.class);
    public static final RedisManager I = new RedisManager();

    private final RedisConnectionFactory factory = new RedisConnectionFactory();

    public void init() {
        log.info("begin init redis...");
        factory.setPassword(CC.mp.redis.password);
        factory.setPoolConfig(CC.mp.redis.getPoolConfig(JedisPoolConfig.class));
        factory.setRedisServers(CC.mp.redis.nodes());
        factory.setModel(CC.mp.redis.mode);
        factory.setSentinelMaster(CC.mp.redis.master);
        factory.setCluster(CC.mp.redis.isCluster());
        factory.init();
        test();
        log.info("init redis success...");
    }

    private <R> R call(Function<JedisCommands, R> function, R d) {
        if (factory.isCluster()) {
            try {
                return function.apply(factory.getClusterConnection());
            } catch (Exception e) {
                log.error("redis ex", e);
            }
        } else {
            try (Jedis jedis = factory.getJedisConnection()) {
                return function.apply(jedis);
            } catch (Exception e) {
                log.error("redis ex", e);
            }
        }
        return d;
    }

    private void call(Consumer<JedisCommands> consumer) {
        if (factory.isCluster()) {
            try {
                consumer.accept(factory.getClusterConnection());
            } catch (Exception e) {
                log.error("redis ex", e);
            }
        } else {
            try (Jedis jedis = factory.getJedisConnection()) {
                consumer.accept(jedis);
            } catch (Exception e) {
                log.error("redis ex", e);
            }
        }
    }

    public long incr(final String key) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.incr(key);
            }
        }, 0L);
    }

    public long incrBy(final String key, final long delt) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.incrBy(key, delt);
            }
        }, 0L);
    }

    /********************* k v redis start ********************************/
    /**
     * @param key
     * @param clazz
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> T get(final String key, Class<T> clazz) {
        String value = call(jedis -> jedis.get(key), null);
        if (value == null) {
            return null;
        } else if (clazz == String.class) {
            return (T) value;
        }
        return Jsons.fromJson(value, clazz);
    }


    public <T extends Number> T getNumber(final String key, Class<T> clazz) {
        String value = call(jedis -> jedis.get(key), null);
        value = value == null ? "0" : value;
        return NumberUtils.parseNumber(value, clazz);
    }

    public void set(String key, String value) {
        set(key, value, 0);
    }

    public void set(String key, Object value) {
        set(key, value, 0);
    }

    public void set(String key, Object value, int time) {
        set(key, Jsons.toJson(value), time);
    }

    /**
     * @param key
     * @param value
     * @param time  seconds
     */
    public void set(final String key, final String value, final int time) {
        call(new Consumer<JedisCommands>() {
            @Override
            public void accept(JedisCommands jedis) {
                jedis.set(key, value);
                if (time > 0) {
                    jedis.expire(key, time);
                }
            }
        });
    }

    public void del(final String key) {
        call(new Consumer<JedisCommands>() {
            @Override
            public void accept(JedisCommands jedis) {
                jedis.del(key);
            }
        });
    }

    /********************* k v redis end ********************************/

    /*********************
     * hash redis start
     ********************************/
    public void hset(final String key, final String field, final String value) {
        call(new Consumer<JedisCommands>() {
            @Override
            public void accept(JedisCommands jedis) {
                jedis.hset(key, field, value);
            }
        });
    }

    public void hset(final String key, final String field, final Object value) {
        hset(key, field, Jsons.toJson(value));
    }

    public String hget(final String key, final String field) {
        return call(new Function<JedisCommands, String>() {
            @Override
            public String apply(JedisCommands jedis) {
                return jedis.hget(key, field);
            }
        }, null);
    }

    @SuppressWarnings("unchecked")
    public <T> T hget(final String key, final String field, Class<T> clazz) {
        String value = call(new Function<JedisCommands, String>() {
            @Override
            public String apply(JedisCommands jedis) {
                return jedis.hget(key, field);
            }
        }, null);
        if (value == null) return null;
        if (clazz == String.class) return (T) value;
        return Jsons.fromJson(value, clazz);
    }

    public void hdel(final String key, final String... field) {
        call(new Consumer<JedisCommands>() {
            @Override
            public void accept(JedisCommands jedis) {
                jedis.hdel(key, field);
            }
        });
    }

    public Map<String, String> hgetAll(final String key) {
        return call(new Function<JedisCommands, Map<String, String>>() {
            @Override
            public Map<String, String> apply(JedisCommands jedis) {
                return jedis.hgetAll(key);
            }
        }, Collections.<String, String>emptyMap());
    }

    public <T> Map<String, T> hgetAll(String key, Class<T> clazz) {
        Map<String, String> result = hgetAll(key);
        if (result.isEmpty()) return Collections.emptyMap();
        Map<String, T> newMap = new HashMap<>(result.size());
        for (Map.Entry<String, String> entry : result.entrySet()) {
            newMap.put(entry.getKey(), Jsons.fromJson(entry.getValue(), clazz));
        }
        return newMap;
    }

    /**
     * 返回 key 指定的哈希集中所有字段的名字。
     *
     * @param key
     * @return
     */
    public Set<String> hkeys(final String key) {
        return call(new Function<JedisCommands, Set<String>>() {
            @Override
            public Set<String> apply(JedisCommands jedis) {
                return jedis.hkeys(key);
            }
        }, Collections.<String>emptySet());
    }

    /**
     * 返回 key 指定的哈希集中指定字段的值
     *
     * @param fields
     * @param clazz
     * @return
     */
    public <T> List<T> hmget(final String key, Class<T> clazz, final String... fields) {
        List<String> list = call(new Function<JedisCommands, List<String>>() {
            @Override
            public List<String> apply(JedisCommands jedis) {
                return jedis.hmget(key, fields);
            }
        }, null);
        List<T> result = new ArrayList<>(list == null ? 0 : list.size());
        if (list == null) {
            return result;
        }
        for (String s : list) {
            result.add(Jsons.fromJson(s, clazz));
        }
        return result;
    }

    /**
     * 设置 key 指定的哈希集中指定字段的值。该命令将重写所有在哈希集中存在的字段。如果 key 指定的哈希集不存在，会创建一个新的哈希集并与 key
     * 关联
     *
     * @param hash
     * @param time
     */
    public void hmset(final String key, final Map<String, String> hash, final int time) {
        call(new Consumer<JedisCommands>() {
            @Override
            public void accept(JedisCommands jedis) {
                jedis.hmset(key, hash);
                if (time > 0) {
                    jedis.expire(key, time);
                }
            }
        });
    }

    public void hmset(final String key, Map<String, String> hash) {
        hmset(key, hash, 0);
    }

    public long hincrBy(final String key, final String field, final long value) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.hincrBy(key, field, value);
            }
        }, 0L);
    }

    /********************* hash redis end ********************************/

    /********************* list redis start ********************************/
    /**
     * 从队列的左边入队
     */
    public void lpush(final String key, final String value) {
        call(new Consumer<JedisCommands>() {
            @Override
            public void accept(JedisCommands jedis) {
                jedis.lpush(key, value);
            }
        });
    }

    public void lpush(final String key, Object value) {
        lpush(key, Jsons.toJson(value));
    }

    /**
     * 从队列的右边入队
     */
    public void rpush(final String key, final String value) {
        call(new Consumer<JedisCommands>() {
            @Override
            public void accept(JedisCommands jedis) {
                jedis.rpush(key, value);
            }
        });
    }

    public void rpush(final String key, Object value) {
        rpush(key, Jsons.toJson(value));
    }

    /**
     * 移除并且返回 key 对应的 list 的第一个元素
     */
    @SuppressWarnings("unchecked")
    public <T> T lpop(final String key, Class<T> clazz) {
        String value = call(new Function<JedisCommands, String>() {
            @Override
            public String apply(JedisCommands jedis) {
                return jedis.lpop(key);
            }
        }, null);
        if (value == null) return null;
        if (clazz == String.class) return (T) value;
        return Jsons.fromJson(value, clazz);
    }

    /**
     * 从队列的右边出队一个元素
     */
    @SuppressWarnings("unchecked")
    public <T> T rpop(final String key, Class<T> clazz) {
        String value = call(new Function<JedisCommands, String>() {
            @Override
            public String apply(JedisCommands jedis) {
                return jedis.rpop(key);
            }
        }, null);
        if (value == null) return null;
        if (clazz == String.class) return (T) value;
        return Jsons.fromJson(value, clazz);
    }

    /**
     * 从列表中获取指定返回的元素 start 和 end
     * 偏移量都是基于0的下标，即list的第一个元素下标是0（list的表头），第二个元素下标是1，以此类推。
     * 偏移量也可以是负数，表示偏移量是从list尾部开始计数。 例如， -1 表示列表的最后一个元素，-2 是倒数第二个，以此类推。
     */
    public <T> List<T> lrange(final String key, final int start, final int end, Class<T> clazz) {
        List<String> list = call(new Function<JedisCommands, List<String>>() {
            @Override
            public List<String> apply(JedisCommands jedis) {
                return jedis.lrange(key, start, end);
            }
        }, null);
        List<T> result = new ArrayList<>(list == null ? 0 : list.size());
        if (list == null) {
            return result;
        }
        for (String s : list) {
            result.add(Jsons.fromJson(s, clazz));
        }
        return result;
    }

    /**
     * 从列表中获取指定返回的元素 start 和 end
     * 偏移量都是基于0的下标，即list的第一个元素下标是0（list的表头），第二个元素下标是1，以此类推。
     * 偏移量也可以是负数，表示偏移量是从list尾部开始计数。 例如， -1 表示列表的最后一个元素，-2 是倒数第二个，以此类推。
     */
    public List<String> lrange(final String key, final int start, final int end) {
        return call(new Function<JedisCommands, List<String>>() {
            @Override
            public List<String> apply(JedisCommands jedis) {
                return jedis.lrange(key, start, end);
            }
        }, Collections.<String>emptyList());

    }

    /**
     * 返回存储在 key 里的list的长度。 如果 key 不存在，那么就被看作是空list，并且返回长度为 0。 当存储在 key
     * 里的值不是一个list的话，会返回error。
     */
    public long llen(final String key) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.llen(key);
            }
        }, 0L);
    }

    /**
     * 移除表中所有与 value 相等的值
     *
     * @param key
     * @param value
     */
    public void lRem(final String key, final String value) {
        call(new Consumer<JedisCommands>() {
            @Override
            public void accept(JedisCommands jedis) {
                jedis.lrem(key, 0, value);
            }
        });
    }

    /********************* list redis end ********************************/

    /*********************
     * mq redis start
     ********************************/

    /*********************
     * set redis start
     ********************************/
    /**
     * @param key
     * @param value
     */
    public void sAdd(final String key, final String value) {
        call(new Consumer<JedisCommands>() {
            @Override
            public void accept(JedisCommands jedis) {
                jedis.sadd(key, value);
            }
        });
    }

    /**
     * @param key
     * @return
     */
    public Long sCard(final String key) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.scard(key);
            }
        }, 0L);
    }

    public void sRem(final String key, final String value) {
        call(new Consumer<JedisCommands>() {
            @Override
            public void accept(JedisCommands jedis) {
                jedis.srem(key, value);
            }
        });
    }

    public boolean sismember(final String key, final String value) {
        return call(new Function<JedisCommands, Boolean>() {
            @Override
            public Boolean apply(JedisCommands jedis) {
                return jedis.sismember(key, value);
            }
        }, false);
    }

    @Override
    public Boolean sismember1(String key, String member) {
        return call(new Function<JedisCommands, Boolean>() {
            @Override
            public Boolean apply(JedisCommands jedis) {
                return jedis.sismember(key, member);
            }
        }, false);
    }

    public Set<String> smembers(final String key) {
        return call(new Function<JedisCommands, Set<String>>() {
            @Override
            public Set<String> apply(JedisCommands jedis) {
                return jedis.smembers(key);
            }
        }, null);
    }

    public String srandmember(final String key) {
        return call(new Function<JedisCommands, String>() {
            @Override
            public String apply(JedisCommands jedis) {
                return jedis.srandmember(key);
            }
        }, null);
    }

    /**
     * 默认使用每页10个
     *
     * @param key
     * @param clazz
     * @return
     */
    public <T> List<T> sScan(final String key, Class<T> clazz, final int start) {
        List<String> list = call(new Function<JedisCommands, List<String>>() {
            @Override
            public List<String> apply(JedisCommands jedis) {
                return jedis.sscan(key, Integer.toString(start), new ScanParams().count(Integer.MAX_VALUE)).getResult();
            }
        }, Collections.<String>emptyList());
        return toList(list, clazz);
    }

    /**
     * 默认使用每页10个
     *
     * @param key
     * @return
     */
    public List<String> sScan(final String key, final int start) {
        return call(new Function<JedisCommands, List<String>>() {
            @Override
            public List<String> apply(JedisCommands jedis) {
                return jedis.sscan(key, Integer.toString(start), new ScanParams().count(Integer.MAX_VALUE)).getResult();
            }
        }, Collections.<String>emptyList());
    }


    /*********************
     * sorted set
     ********************************/
    /**
     * @param key
     * @param value
     */
    public void zAdd(final String key, final String value) {
        call(new Consumer<JedisCommands>() {
            @Override
            public void accept(JedisCommands jedis) {
                jedis.zadd(key, 0, value);
            }
        });
    }

    public void zadd(final String key, final double score, final String member) {
        call(new Consumer<JedisCommands>() {
            @Override
            public void accept(JedisCommands jedis) {
                jedis.zadd(key, score, member);
            }
        });
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.zadd(key, scoreMembers);
            }
        }, 0L);
    }

    @Override
    public Double zincrby(String key, double score, String member) {
        return call(new Function<JedisCommands, Double>() {
            @Override
            public Double apply(JedisCommands jedis) {
                return jedis.zincrby(key, score, member);
            }
        }, null);
    }

    @Override
    public Double zscore(String key, String member) {
        return call(new Function<JedisCommands, Double>() {
            @Override
            public Double apply(JedisCommands jedis) {
                return jedis.zscore(key, member);
            }
        }, null);
    }

    /**
     * @param key
     * @return
     */
    public Long zCard(final String key) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.zcard(key);
            }
        }, 0L);
    }

    @Override
    public Long zcard(String key) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.zcard(key);
            }
        }, 0L);
    }

    @Override
    public Long zcount(String key, double min, double max) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.zcount(key, min, max);
            }
        }, 0L);
    }

    public void zRem(final String key, final String value) {
        call(new Consumer<JedisCommands>() {
            @Override
            public void accept(JedisCommands jedis) {
                jedis.zrem(key, value);
            }
        });
    }

    @Override
    public Long zrem(String key, String... member) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.zrem(key, member);
            }
        }, null);
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.zremrangeByRank(key, start, end);
            }
        }, null);
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.zremrangeByScore(key, start, end);
            }
        }, null);
    }

    @Override
    public Long zrank(String key, String member) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.zrank(key, member);
            }
        }, null);
    }

    @Override
    public Long zrevrank(String key, String member) {
        return call(new Function<JedisCommands, Long>() {
            @Override
            public Long apply(JedisCommands jedis) {
                return jedis.zrevrank(key, member);
            }
        }, null);
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        return call(new Function<JedisCommands, Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisCommands jedis) {
                return jedis.zrangeWithScores(key, start, end);
            }
        }, null);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        return call(new Function<JedisCommands, Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisCommands jedis) {
                return jedis.zrevrangeWithScores(key, start, end);
            }
        }, null);
    }

    /**
     * 重复设置时，key 已经存在返回0,不存在返回1
     *
     * @param key
     * @param value
     * @param <T>
     * @return
     */
    public <T> long setNotExists(String key, T value, int expireTime) {
        return call(x -> {
            long result = x.setnx(key, Jsons.toJson(value));
            if (expireTime > 0 && result == 1) {
                x.expire(key, expireTime);
            }
            return result;
        }, 0L);

    }

    /**
     * 从列表中获取指定返回的元素 start 和 end
     * 偏移量都是基于0的下标，即list的第一个元素下标是0（list的表头），第二个元素下标是1，以此类推。
     * 偏移量也可以是负数，表示偏移量是从list尾部开始计数。 例如， -1 表示列表的最后一个元素，-2 是倒数第二个，以此类推。
     */
    public <T> List<T> zrange(final String key, final int start, final int end, Class<T> clazz) {
        Set<String> value = call(new Function<JedisCommands, Set<String>>() {
            @Override
            public Set<String> apply(JedisCommands jedis) {
                return jedis.zrange(key, start, end);
            }
        }, Collections.<String>emptySet());
        return toList(value, clazz);
    }

    private <T> List<T> toList(Collection<String> value, Class<T> clazz) {
        if (value != null) {
            List<T> newValue = Lists.newArrayList();
            for (String temp : value) {
                newValue.add(Jsons.fromJson(temp, clazz));
            }
            return newValue;
        }
        return null;
    }

    public void destroy() {
        if (factory != null) factory.destroy();
    }

    public void test() {
        if (factory.isCluster()) {
            JedisCluster cluster = factory.getClusterConnection();
            if (cluster == null) throw new RuntimeException("init redis cluster error.");
        } else {
            Jedis jedis = factory.getJedisConnection();
            if (jedis == null) throw new RuntimeException("init redis error, can not get connection.");
            jedis.close();
        }
    }

    public interface Consumer<T> {
        void accept(T var1);
    }
}

