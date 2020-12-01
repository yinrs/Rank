package com.yinrs.rank.redisrank;


import com.yinrs.rank.RankManagerFactory;
import com.yinrs.rank.RankNode;

import java.util.Calendar;
import java.util.Date;
import java.util.List;


public class RedisRankTest {
    public static void main(String[] args) throws Exception {
        RedisRankManager rankManager = RankManagerFactory.getInstance(RedisRankManager.class);
        for (int i = 0; i < 99; i++) {
            test(rankManager, i);
        }
        System.out.println();
    }

    private static void test(RedisRankManager rankManager, int k) {
        RedisRank rank = rankManager.register("rank-" + k);
        for (int i = 0; i < 99; i++) {
            rank.insertOrUpdate("nodeId" + i, 123.45);
        }
        List<RankNode> desc = rank.descRangeByRankWithScores(0, -1);

        String orgNodeId = "nodeId13";
        Double nodeVal1 = rank.getNodeVal(orgNodeId);
        rank.incr(orgNodeId, 123);
        Double nodeVal2 = rank.getNodeVal(orgNodeId);
        rank.remove("nodeId2");
        rank.insertOrUpdate("node999", 123.45);
        Long descRank = rank.getDESCRank(orgNodeId);
        Long aLong = rank.countAll();
        Long aLong1 = rank.countBy(0, 99);
        String rankName = rank.getRankName();
        desc = rank.descRangeByRankWithScores(0, -1);
        System.out.println();
    }

    private static Date getDate(int i) {
        Calendar instance = Calendar.getInstance();
        instance.set(Calendar.MINUTE, i);
        return instance.getTime();
    }
}

