package com.yinrs.rank;

public class RankNode {
    private String nodeId;
    private Long rank;
    private double nodeScore;

    public RankNode(String nodeId, Long rank, double nodeScore) {
        this.nodeId = nodeId;
        this.rank = rank;
        this.nodeScore = nodeScore;
    }

    public String getNodeId() {
        return nodeId;
    }

    public double getNodeScore() {
        return nodeScore;
    }

    public Long getRank() {
        return rank;
    }


    @Override
    public String toString() {
        return "RankNode{" +
                "nodeId='" + nodeId + '\'' +
                ", rank=" + rank +
                ", nodeScore=" + nodeScore +
                '}';
    }
}

