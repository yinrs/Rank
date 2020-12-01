package com.yinrs.rank.redisrank;


import com.yinrs.cache.redis.RedisManager;
import com.yinrs.rank.RankManager;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class RedisRankManager<T extends RedisRank> implements RankManager<T> {
    private static RedisRankManager singleton = null;
    private final static String RANKNAME_PREFIX = "redisrankd8f9s0k1_";
    private final static String Java_Redis_RankORM_Key = "Java_Redis_RankORM_Key";
    private String rankName_prefix;
    private RedisManager redis;
    private JavaRedisRankORM orm;
    private Class<T> rankClass;

    public static RedisRankManager instance() {
        if (singleton == null) {
            synchronized (RedisRankManager.class) {
                if (singleton == null) {
                    singleton = new RedisRankManager(RANKNAME_PREFIX, Java_Redis_RankORM_Key, RedisRank.class);
                }
            }
        }
        return singleton;
    }

    RedisRankManager(String rankName_prefix, String javaRedisRankORM_Key, Class<T> rankClass) {
        RedisManager.I.init();
        redis = RedisManager.I;
        this.rankName_prefix = rankName_prefix;
        this.rankClass = rankClass;
        orm = new JavaRedisRankORM(javaRedisRankORM_Key);
    }

    @Override
    public synchronized T register(String rankName) {
        if (StringUtils.isEmpty(rankName)) return null;
        rankName = wrapRankName(rankName);

        if (orm.contains(rankName)) {
            return orm.get(rankName);
        } else {
            T rank = getRedisRank(rankName);
            orm.register(rankName, rank);//redis memory
            return rank;
        }
    }

    @Override
    public List<T> ranks() {
        Collection<T> values = orm.all();
        if (!CollectionUtils.isEmpty(values)) {
            List<T> list = new ArrayList<>();
            Iterator<T> iterator = values.iterator();
            while (iterator.hasNext()) {
                T next = iterator.next();
                list.add(next);
            }
            return list;
        }
        return null;
    }

    @Override
    public Boolean isRank(String rankName) {
        if (StringUtils.isEmpty(rankName)) return false;
        rankName = wrapRankName(rankName);

        return orm.contains(rankName);
    }

    @Override
    public Boolean isRank(T rank) {
        return orm.contains(rank);
    }

    @Override
    public T find(String rankName) {
        if (StringUtils.isEmpty(rankName)) return null;
        rankName = wrapRankName(rankName);

        return orm.get(rankName);
    }

    @Override
    public void remove(String rankName) {
        if (StringUtils.isEmpty(rankName)) return;
        rankName = wrapRankName(rankName);

        if (orm.contains(rankName)) {
            T redisRank = orm.get(rankName);
            redisRank.removeAll();
            orm.remove(rankName);
        } else {
            T redisRank = getRedisRank(rankName);
            redisRank.removeAll();
        }
    }

    @Override
    public void remove(T rank) {
        if (Objects.isNull(rank)) return;
        String rankName = rank.getRankName();
        if (orm.contains(rankName)) {
            T redisRank = orm.get(rankName);
            redisRank.removeAll();
            orm.remove(rankName);
        } else {
            T redisRank = getRedisRank(rankName);
            redisRank.removeAll();
        }
    }

    @Override
    public void removeAll() {
        for (T rank : ranks()) {
            remove(rank);
        }
    }

    @Override
    public void move(T src, T tgt, String nodeId) {
        if (isRank(src) && isRank(tgt) && !src.equals(tgt)) {
            boolean srcFirst = src.getRankName().compareTo(tgt.getRankName()) > 0;
            if (srcFirst) {
                src.wLock();
                tgt.wLock();
            } else {
                tgt.wLock();
                src.wLock();
            }
            try {
                Double nodeVal = src.getNodeValWithoutLock(nodeId);
                if (Objects.nonNull(nodeVal)) {
                    tgt.insertOrUpdate(nodeId, nodeVal);
                    src.remove(nodeId);
                }
            } finally {
                if (srcFirst) {
                    tgt.wUnlock();
                    src.wUnlock();
                } else {
                    src.wUnlock();
                    tgt.wUnlock();
                }
            }
        }
    }

    private String wrapRankName(String rankName) {
        rankName = rankName.trim();
        if (!rankName.contains(rankName_prefix)) {
            rankName = rankName_prefix + rankName;
        }
        return rankName;
    }

    protected T getRedisRank(String rankName) {
        try {
            Method instance = rankClass.getDeclaredMethod("getInstance", RedisManager.class, String.class);
            instance.setAccessible(true);
            return (T) instance.invoke(null, redis, rankName);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } finally {
        }
        return null;
    }

    private class JavaRedisRankORM {
        private Map<String, T> map;
        private String key;

        JavaRedisRankORM(String key) {
            this.map = new ConcurrentHashMap<>();
            this.key = key;
            clearORM();
        }

        private void clearORM() {
            Set<String> all = redis.smembers(key);
            if (!CollectionUtils.isEmpty(all)) {
                for (String rankName : all) {
                    T rank = getRedisRank(rankName);
                    rank.removeAll();
                    redis.sRem(key, rankName);
                    map.remove(rankName);
                }
            }
        }

        public void register(String rankName, T redisRank) {
            redis.sAdd(key, rankName);
            map.put(rankName, redisRank);
        }

        public void remove(String rankName) {
            redis.sRem(key, rankName);
            map.remove(rankName);
        }

        public boolean contains(String rankName) {
            return map.containsKey(rankName);
        }

        public boolean contains(T rank) {
            if (Objects.nonNull(rank) && !StringUtils.isEmpty(rank.getRankName())) {
                return contains(rank.getRankName());
            }
            return false;
        }

        public T get(String rankName) {
            return map.get(rankName);
        }

        public Collection<T> all() {
            return map.values();
        }
    }
}
