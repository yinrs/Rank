package com.yinrs.rank;

import java.util.List;

public abstract class Rank {
    private String rankName;

    protected Rank(String rankName) {
        this.rankName = rankName;
    }

    public abstract void insertOrUpdate(String nodeId, double nodeScores);

    public abstract Long insertOrUpdate(List<RankNode> list);

    public abstract Double incr(String nodeId, double nodeScores);

    public abstract Double getNodeVal(String nodeId);

    public abstract long remove(String... nodeIds);

    public abstract long removeByRank(long start, long end);//start with 0

    public abstract long removeByScore(double minNodeScores, double maxNodeScores);

    public abstract Long getASCRank(String nodeId);//start with 0

    public abstract Long getDESCRank(String nodeId);//start with 0

    public abstract List<RankNode> ascRangeByRankWithScores(long start, long end);//start with 0

    public abstract List<RankNode> descRangeByRankWithScores(long start, long end);//start with 0

    public abstract Long countAll();

    public abstract Long countBy(double minNodeScores, double maxNodeScores);

    public abstract Long removeAll();

    public String getRankName() {
        return rankName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof Rank)) return false;
        Rank rank = (Rank) o;
        return rankName.equals(rank.rankName);
    }
}

