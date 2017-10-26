package com.commandcenter.commandcenter;

/**
 * Created by Seongsoo on 2017-05-23.
 */

public class StandWeightSet implements Comparable<StandWeightSet> {

    int category;
    int weightSum;

    @Override
    public int compareTo(StandWeightSet standWeightSet) {
        if(this.weightSum < standWeightSet.weightSum)
            return -1;
        else if(this.weightSum == standWeightSet.weightSum)
            return 0;
        else
            return 1;
    }
}
