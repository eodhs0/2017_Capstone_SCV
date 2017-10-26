package com.commandcenter.commandcenter;

/**
 * Created by Seongsoo on 2017-05-23.
 */

import java.util.Arrays;
import java.util.concurrent.SynchronousQueue;

public class PathFinder {

    // 생성자로 맵에 표시할 카테고리들을 받아온다.

    String[] testString = new String[13];
    StandWeightSet[] minStandWeightSort;
    int[] standArray;

    public PathFinder() {
        minStandWeightSort = new StandWeightSet[testString.length + 2];
    }

    // 해당 카테고리들로 가기 위한 경로를 추적하고, 해당 경로들을 배열에 저장.
    public void findPathWithFloyd(MapPath_Floyd map, SaleStandSet saleStandSet, String[] category) {

        testString = category;
        for(int i = 0 ;i <testString.length; i++)
            System.out.printf("%s ", testString[i]);
        System.out.println();
        for (int i = 0; i < minStandWeightSort.length; i++) {
            minStandWeightSort[i] = new StandWeightSet();
        }

        minStandWeightSort[0].category = 0;
        minStandWeightSort[0].weightSum = map.distanceArray[0][1];
        minStandWeightSort[minStandWeightSort.length - 1].category = 20;
        minStandWeightSort[minStandWeightSort.length - 1].weightSum = map.distanceArray[0][21];

        for (int i = 0; i < testString.length; i++) {
            for (int j = 0; j < 24; j++) {
                if (testString[i].equals(saleStandSet.saleStands[j].getSaleCategory().toString())) {
                    minStandWeightSort[i + 1].category = j;
                    minStandWeightSort[i + 1].weightSum = map.distanceArray[1][j];
                    break;
                }
            }
        }

        System.out.printf("TEST_1 ");
        for(int i = 0; i<minStandWeightSort.length; i++) {
            System.out.printf("%d ", minStandWeightSort[i].weightSum);
        }
        System.out.println();

        Arrays.sort(minStandWeightSort);

        System.out.printf("TEST_2 ");
        for(int i = 0; i<minStandWeightSort.length; i++) {
            System.out.printf("%d ", minStandWeightSort[i].weightSum);
        }
        System.out.println();

        int[] trackingStandArray;
        int trackingIndex = 0;
        int standIndex = 1;
        standArray = new int[24];
        standArray[0] = 0;

        for(int i = 0; i<minStandWeightSort.length-1; i++) {
            trackingStandArray = new int[24];
            trackingPath(map.pathArray, minStandWeightSort[i].category, minStandWeightSort[i + 1].category, trackingStandArray, trackingIndex);
            for (int j = 23; j > 0; j--) {
                if (trackingStandArray[j] != 0) {
                    standArray[standIndex++] = trackingStandArray[j];
                }
            }
            standArray[standIndex++] = minStandWeightSort[i+1].category;
        }
        System.out.printf("TEST_3 ");
        for(int i = 0; i<standArray.length; i++) {
            System.out.printf("%d ", standArray[i]);
        }
        System.out.println();

    }

    // recursive를 활용하여 맵의 경로를 추적
    public void trackingPath(int[][] pathArray, int startStand, int endStand, int[] trackingStandArray, int trackingIndex) {

        if(pathArray[startStand][endStand] != 0) {
            trackingPath(pathArray, startStand, pathArray[startStand][endStand], trackingStandArray, ++trackingIndex);
            trackingStandArray[trackingIndex] = pathArray[startStand][endStand];
            trackingPath(pathArray, pathArray[startStand][endStand], endStand, trackingStandArray, ++trackingIndex);
        }
    }
}
