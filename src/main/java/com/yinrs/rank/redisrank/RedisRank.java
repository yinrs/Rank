package com.yinrs.rank.redisrank;

import com.yinrs.cache.redis.RedisManager;
import com.yinrs.rank.Rank;
import com.yinrs.rank.RankNode;
import org.springframework.util.CollectionUtils;
import redis.clients.jedis.Tuple;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class RedisRank extends Rank {
    private RedisManager redis;
    private ReadWriteLock rwLock;
    private Lock rLock;
    private Lock wLock;

    RedisRank(RedisManager redis, String rankName) {
        super(rankName);
        this.redis = redis;
        initLock();
    }

    static RedisRank getInstance(RedisManager redis, String rankName) {
        return new RedisRank(redis, rankName);
    }

    private void initLock() {
        rwLock = new ReentrantReadWriteLock();
        rLock = rwLock.readLock();
        wLock = rwLock.writeLock();
    }

    @Override
    public void insertOrUpdate(String nodeId, double nodeScores) {
        wLock();
        try {
            redis.zadd(getRankName(), nodeScores, nodeId);
        } finally {
            wUnlock();
        }
    }

    @Override
    public Long insertOrUpdate(List<RankNode> list) {
        wLock();
        try {
            if (!CollectionUtils.isEmpty(list)) {
                Map<String, Double> map = new HashMap<>();
                for (int i = 0; i < list.size(); i++) {
                    map.put(list.get(i).getNodeId(), list.get(i).getNodeScore());
                }
                return redis.zadd(getRankName(), map);
            }
        } finally {
            wUnlock();
        }
        return null;
    }

    @Override
    public Double incr(String nodeId, double nodeScores) {
        wLock();
        try {
            return redis.zincrby(getRankName(), nodeScores, nodeId);
        } finally {
            wUnlock();
        }
    }

    @Override
    public Double getNodeVal(String nodeId) {
        rLock();
        try {
            return redis.zscore(getRankName(), nodeId);
        } finally {
            rUnlock();
        }
    }

    protected Double getNodeValWithoutLock(String nodeId) {
        return redis.zscore(getRankName(), nodeId);
    }

    @Override
    public long remove(String... nodeIds) {
        wLock();
        try {
            return redis.zrem(getRankName(), nodeIds);
        } finally {
            wUnlock();
        }
    }

    @Override
    public long removeByRank(long start, long end) {
        wLock();
        try {
            return redis.zremrangeByRank(getRankName(), start, end);
        } finally {
            wUnlock();
        }
    }

    @Override
    public long removeByScore(double minNodeScores, double maxNodeScores) {
        wLock();
        try {
            return redis.zremrangeByScore(getRankName(), minNodeScores, maxNodeScores);
        } finally {
            wUnlock();
        }
    }

    @Override
    public Long getASCRank(String nodeId) {
        rLock();
        try {
            return redis.zrank(getRankName(), nodeId);
        } finally {
            rUnlock();
        }
    }

    @Override
    public Long getDESCRank(String nodeId) {
        rLock();
        try {
            return redis.zrevrank(getRankName(), nodeId);
        } finally {
            rUnlock();
        }
    }

    @Override
    public List<RankNode> ascRangeByRankWithScores(long start, long end) {
        rLock();
        try {
            Set<Tuple> tuples = redis.zrangeWithScores(getRankName(), start, end);
            if (Objects.nonNull(tuples)) {
                Iterator<Tuple> iterator = tuples.iterator();
                List<RankNode> list = new LinkedList<>();
                while (iterator.hasNext()) {
                    Tuple next = iterator.next();
                    list.add(new RankNode(next.getElement(), start++, next.getScore()));
                }
                return list;
            }
        } finally {
            rUnlock();
        }
        return null;
    }

    @Override
    public List<RankNode> descRangeByRankWithScores(long start, long end) {
        rLock();
        try {
            Set<Tuple> tuples = redis.zrevrangeWithScores(getRankName(), start, end);
            if (Objects.nonNull(tuples)) {
                Iterator<Tuple> iterator = tuples.iterator();
                List<RankNode> list = new LinkedList<>();
                while (iterator.hasNext()) {
                    Tuple next = iterator.next();
                    list.add(new RankNode(next.getElement(), start++, next.getScore()));
                }
                return list;
            }
        } finally {
            rUnlock();
        }
        return null;
    }

    @Override
    public Long countAll() {
        rLock();
        try {
            return redis.zcard(getRankName());
        } finally {
            rUnlock();
        }
    }

    @Override
    public Long countBy(double minNodeScores, double maxNodeScores) {
        rLock();
        try {
            return redis.zcount(getRankName(), minNodeScores, maxNodeScores);
        } finally {
            rUnlock();
        }
    }

    @Override
    public Long removeAll() {
        wLock();
        try {
            return removeByRank(0, -1);
        } finally {
            wUnlock();
        }
    }

    protected RedisManager getRedis() {
        return redis;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    protected void wLock() {
        wLock.lock();
    }

    protected void wUnlock() {
        wLock.unlock();
    }

    protected void rLock() {
        rLock.lock();
    }

    protected void rUnlock() {
        rLock.unlock();
    }
}

