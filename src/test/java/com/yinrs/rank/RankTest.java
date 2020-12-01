package com.yinrs.rank;

import com.yinrs.rank.redisrank.RedisRankManager;

import java.util.*;

public class RankTest {

    private static String rankNamePre = "rank-";
    private static String nodeIdPre = "-nodeId-";

    public static void main(String[] args) {
        Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        RankManager manager = RankManagerFactory.getInstance(RedisRankManager.class);

        Map<Integer, Rank> rankMap = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Rank rank = manager.register(rankNamePre + i);
            rankMap.put(i, rank);
            for (int j = 0; j < 10; j++) {
                rank.insertOrUpdate(rank.getRankName() + nodeIdPre + j, random.nextDouble());
            }
        }

        List<Rank> ranks = manager.ranks();
        for (Rank rank : ranks) {
            print(rank, true);
        }

        manager.remove(rankMap.get(0));
        manager.remove(rankMap.get(1));
        manager.remove(rankMap.get(2));
        manager.remove(rankMap.get(3));
        manager.remove(rankMap.get(4));

        ranks = manager.ranks();

        manager.move(ranks.get(ranks.size() - 2), ranks.get(ranks.size() - 1), ranks.get(ranks.size() - 2).getRankName() + nodeIdPre + 1);
        for (Rank rank : ranks) {
            print(rank, true);
        }

        Rank rank5 = rankMap.get(5);
        System.out.println(rank5.getASCRank(rank5.getRankName() + nodeIdPre + 1));
        System.out.println(rank5.getDESCRank(rank5.getRankName() + nodeIdPre + 1));
        rank5.incr(rank5.getRankName() + nodeIdPre + 1, 1);
        print(rank5, true);
        print(rank5, false);
        System.out.println(rank5.countAll());
        System.out.println(rank5.countBy(0, 0.5));
        rank5.removeByScore(0, 0.3);
        print(rank5, true);

        List<RankNode> rankNodes = rank5.ascRangeByRankWithScores(0, -1);
        for (RankNode rankNode : rankNodes) {
            //rankNode.setNodeId(rankNode.getNodeId() + "copy");
        }

        rank5.insertOrUpdate(rankNodes);
        print(rank5, true);

        List<Rank> list = manager.ranks();

        manager.removeAll();
        list = manager.ranks();
        System.out.println();
    }

    private static void print(Rank rank, boolean isAsc) {
        System.out.println(rank.getRankName() + ":");
        List<RankNode> rankNodes = isAsc ? rank.ascRangeByRankWithScores(0, -1) : rank.descRangeByRankWithScores(0, -1);
        for (RankNode rankNode : rankNodes) {
            System.out.println(rankNode);
        }
        System.out.println("\n\n");
    }
}

