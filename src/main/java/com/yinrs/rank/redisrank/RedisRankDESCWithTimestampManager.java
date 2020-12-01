package com.yinrs.rank.redisrank;

import com.yinrs.rank.RankManager;

public class RedisRankDESCWithTimestampManager<T extends RedisRankDESCWithTimestamp> extends RedisRankManager<T> implements RankManager<T> {
    private static RedisRankDESCWithTimestampManager singleton = null;
    private final static String RANKNAME_PREFIX = "redisrankwithtimestampk2h5s9e7_";
    private final static String RankNameSet_Key = "RedisRankDESCWithTimestamp_RankName_Set_Key";

    public static RedisRankDESCWithTimestampManager instance() {
        if (singleton == null) {
            synchronized (RedisRankDESCWithTimestampManager.class) {
                if (singleton == null) {
                    singleton = new RedisRankDESCWithTimestampManager(RANKNAME_PREFIX, RankNameSet_Key, RedisRankDESCWithTimestamp.class);
                }
            }
        }
        return singleton;
    }

    RedisRankDESCWithTimestampManager(String rankName_prefix, String rankNameSet_Key, Class<T> clazz) {
        super(rankName_prefix, rankNameSet_Key, clazz);
    }

    @Override
    @Deprecated
    public void move(T src, T tgt, String nodeId) {
        throw new UnsupportedOperationException();
    }
}
