package com.yinrs.rank;

import java.util.List;

public interface RankManager<T extends Rank> {
    T register(String rankName);

    List<T> ranks();

    Boolean isRank(String rankName);

    Boolean isRank(T rank);

    T find(String rankName);

    void remove(String rankName);

    void remove(T rank);

    void removeAll();

    void move(T src, T tgt, String nodeId);
}

