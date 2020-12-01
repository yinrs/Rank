package com.yinrs.rank.redisrank;


import com.yinrs.rank.RankNode;

import java.util.Date;

public class RankNodeWithTimestamp extends RankNode {
    private Date date;

    public RankNodeWithTimestamp(String nodeId, long rank, double nodeScore, Date date) {
        super(nodeId, rank, nodeScore);
        this.date = date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
