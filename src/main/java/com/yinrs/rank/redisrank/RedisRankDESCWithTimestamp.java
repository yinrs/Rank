package com.yinrs.rank.redisrank;


import com.yinrs.cache.redis.RedisManager;
import com.yinrs.rank.RankNode;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Tuple;

import java.util.*;


public class RedisRankDESCWithTimestamp extends RedisRank {
    private NodeIdAndDateManager nodeIdManager;

    RedisRankDESCWithTimestamp(RedisManager redis, String rankName) {
        super(redis, rankName);
        nodeIdManager = new NodeIdAndDateManager(getRedis(), rankName);
    }

    static RedisRankDESCWithTimestamp getInstance(RedisManager redis, String rankName) {
        return new RedisRankDESCWithTimestamp(redis, rankName);
    }

    @Override
    @Deprecated
    public void insertOrUpdate(String nodeId, double nodeScores) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Long insertOrUpdate(List<RankNode> list) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Double incr(String nodeId, double nodeScores) {
        throw new UnsupportedOperationException();
    }

    public void insertOrUpdate(String nodeId, double nodeScores, Date date) {
        wLock();
        try {
            String[] compNodeIds = nodeIdManager.updateCompNodeId(nodeId, date);
            if (!StringUtils.isEmpty(compNodeIds[0])) {
                super.remove(compNodeIds[0]);
            }
            super.insertOrUpdate(compNodeIds[1], nodeScores);
        } finally {
            wUnlock();
        }
    }

    public Double incr(String nodeId, double nodeScores, Date date) {
        wLock();
        try {
            String[] compNodeIds = nodeIdManager.updateCompNodeId(nodeId, date);
            Double oldVal = super.getNodeValWithoutLock(compNodeIds[0]);
            if (!StringUtils.isEmpty(compNodeIds[0])) {
                super.remove(compNodeIds[0]);
            }
            double finalVal = Objects.nonNull(oldVal) ? oldVal + nodeScores : nodeScores;
            super.insertOrUpdate(compNodeIds[1], finalVal);
            return finalVal;
        } finally {
            wUnlock();
        }
    }

    public Double getNodeVal(String nodeId) {
        rLock();
        try {
            String oldCompNodeId = nodeIdManager.getOldCompNodeId(nodeId);
            return super.getNodeVal(oldCompNodeId);
        } finally {
            rUnlock();
        }
    }

    public long remove(String... nodeIds) {
        wLock();
        try {
            if (nodeIds == null || nodeIds.length == 0) return 0;
            String[] oldCompNodeId = new String[nodeIds.length];
            for (int i = 0; i < nodeIds.length; i++) {
                oldCompNodeId[i] = nodeIdManager.getOldCompNodeId(nodeIds[i]);
            }
            nodeIdManager.remove(nodeIds);//redis manager memory
            return super.remove(oldCompNodeId);
        } finally {
            wUnlock();
        }
    }

    @Deprecated
    public long removeByRank(long start, long end) {//start with 0
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public long removeByScore(double minNodeScores, double maxNodeScores) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public Long getASCRank(String nodeId) {//start with 0
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public RankNodeWithTimestamp getASCRankNode(String nodeId) {//start with 0
        throw new UnsupportedOperationException();
    }

    public Long getDESCRank(String nodeId) {//start with 0
        rLock();
        try {
            String oldCompNodeId = nodeIdManager.getOldCompNodeId(nodeId);
            return super.getDESCRank(oldCompNodeId);
        } finally {
            rUnlock();
        }
    }

    public RankNodeWithTimestamp getDESCRankNode(String nodeId) {//start with 0
        rLock();
        try {
            String oldCompNodeId = nodeIdManager.getOldCompNodeId(nodeId);
            Long descRank = super.getDESCRank(oldCompNodeId);
            Double scores = super.getNodeVal(oldCompNodeId);
            return new RankNodeWithTimestamp(nodeId, descRank, scores, NodeIdAndDateManager.outputDate(oldCompNodeId));
        } finally {
            rUnlock();
        }
    }

    @Deprecated
    public List<RankNode> ascRangeByRankWithScores(long start, long end) {//start with 0
        throw new UnsupportedOperationException();
    }

    public List<RankNode> descRangeByRankWithScores(long start, long end) {//start with 0
        rLock();
        try {
            Set<Tuple> tuples = getRedis().zrevrangeWithScores(getRankName(), start, end);
            if (Objects.nonNull(tuples)) {
                Iterator<Tuple> iterator = tuples.iterator();
                List<RankNode> list = new LinkedList<>();
                while (iterator.hasNext()) {
                    Tuple next = iterator.next();
                    String compNodeId = next.getElement();
                    list.add(new RankNodeWithTimestamp(NodeIdAndDateManager.outputNodeId(compNodeId), start++, next.getScore(), NodeIdAndDateManager.outputDate(compNodeId)));
                }
                return list;
            }
        } finally {
            rUnlock();
        }
        return null;
    }

    //public abstract Long countAll();
    //public abstract Long countBy(double minNodeScores, double maxNodeScores);

    public Long removeAll() {
        wLock();
        try {
            nodeIdManager.removeAll();
            return getRedis().zremrangeByRank(getRankName(), 0, -1);
        } finally {
            wUnlock();
        }
    }

    /**
     * warning:this class is not thread safe!
     * must be used in RedisRankWithTimestamp with ReadWriteLock.
     */
    private static class NodeIdAndDateManager {
        private RedisManager redis;
        private String key;

        public NodeIdAndDateManager(RedisManager redis, String key) {
            this.redis = redis;
            this.key = key + "-hash";
        }

        public String getOldCompNodeId(String orgNodeId) {
            return redis.hget(key, orgNodeId);
        }

        public String[] getAllCompNodeId() {
            Map<String, String> map = redis.hgetAll(key);
            String[] nodeIds = new String[map.size()];
            Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
            int i = 0;
            while (iterator.hasNext()) {
                nodeIds[i] = iterator.next().getValue();
            }
            return nodeIds;
        }

        public String[] updateCompNodeId(String orgNodeId, Date date) {
            String oldCompNodeId = getOldCompNodeId(orgNodeId);
            String newCompNodeId = invertTime(date.getTime()) + "_" + orgNodeId;
            if (!StringUtils.isEmpty(oldCompNodeId)) {
                remove(orgNodeId);
            }
            redis.hset(key, orgNodeId, newCompNodeId);
            return new String[]{oldCompNodeId, newCompNodeId};
        }

        public void remove(String... orgNodeId) {
            if (Objects.nonNull(orgNodeId) && orgNodeId.length > 0) {
                List<String> list = new LinkedList<>();
                for (int i = 0; i < orgNodeId.length; i++) {
                    if (!StringUtils.isEmpty(orgNodeId[i])) {
                        list.add(orgNodeId[i]);
                    }
                }
                if (list.size() > 0) {
                    orgNodeId = new String[list.size()];
                    for (int i = 0; i < list.size(); i++) {
                        orgNodeId[i] = list.get(i);
                    }
                    redis.hdel(key, orgNodeId);
                }
            }
        }

        public void removeAll() {
            Map<String, String> map = redis.hgetAll(key);
            String[] orgNodeIds = new String[map.size()];
            Iterator<String> iterator = map.keySet().iterator();
            int i = 0;
            while (iterator.hasNext()) {
                orgNodeIds[i] = iterator.next();
            }
            remove(orgNodeIds);
        }

        public static Date outputDate(String compNodeId) {
            return new Date(invertTime(Long.valueOf(compNodeId.substring(0, compNodeId.indexOf("_")))));
        }

        public static String outputNodeId(String compNodeId) {
            return compNodeId.substring(compNodeId.indexOf("_") + 1);
        }

        private static Long invertTime(long time) {
            return 99999999999999l - time;
        }
    }
}

