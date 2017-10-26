package com.commandcenter.commandcenter;

/**
 * Created by Seongsoo on 2017-05-23.
 */

public class MapPath_Floyd {

    private static int nodeCount = 24;
    int INF = 999; // 연결되지 않은 경로. 갈 수 없는 경로의 가중치 값.

    int distanceArray[][] = new int[nodeCount][nodeCount]; // 지점과 지점 사이 최단 경로 값을 저장
    int pathArray[][] = new int[nodeCount][nodeCount]; // 경유지점을 저장
    int weightArray[][] = new int[nodeCount][nodeCount]; // 초기 지도의 가중치 값을 저장

    public MapPath_Floyd() { // 생성자로 가중치 배열을 초기화
        weightArray = new int[][]{
                {0, 10, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF}, //0
                {10, 0, 10, INF, INF, 5, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF}, //1
                {INF, 10, 0, 10, INF, INF, 5, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF}, //2
                {INF, INF, 10, 0, INF, INF, INF, 10, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF}, //3
                {INF, INF, INF, INF, 0, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF}, //4
                {INF, 5, INF, INF, INF, 0, 10, INF, INF, 15, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF}, //5
                {INF, INF, 5, INF, INF, 10, 0, 10, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF}, //6
                {INF, INF, INF, 10, INF, INF, 10, 0, INF, INF, INF, 10, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF}, //7
                {INF, INF, INF, INF, INF, INF, INF, INF, 0, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF}, //8
                {INF, INF, INF, INF, INF, 15, INF, INF, INF, 0, 10, INF, INF, 10, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF}, //9
                {INF, INF, INF, INF, INF, INF, INF, INF, INF, 10, 0, 15, INF, INF, 5, INF, INF, INF, INF, INF, INF, INF, INF, INF}, //10
                {INF, INF, INF, INF, INF, INF, INF, 10, INF, INF, 15, 0, INF, INF, INF, 5, INF, INF, INF, INF, INF, INF, INF, INF}, //11
                {INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, 0, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF}, //12
                {INF, INF, INF, INF, INF, INF, INF, INF, INF, 10, INF, INF, INF, 0, 10, INF, INF, 10, INF, INF, INF, INF, INF, INF}, //13
                {INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, 5, INF, INF, 10, 0, 10, INF, INF, INF, INF, INF, INF, INF, INF}, //14
                {INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, 5, INF, INF, 10, 0, INF, INF, INF, 10, INF, INF, INF, INF}, //15
                {INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, 0, INF, INF, INF, INF, INF, INF, INF}, //16
                {INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, 10, INF, INF, INF, 0, 10, INF, INF, 40, INF, INF}, //17
                {INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, 10, 0, 10, INF, INF, 20, INF}, //18
                {INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, 10, INF, INF, 10, 0, INF, INF, INF, 10}, //19
                {INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, 0, 10, INF, INF}, //20
                {INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, 40, INF, INF, 10, 0, 10, INF}, //21
                {INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, 20, INF, INF, 10, 0, 10}, //22
                {INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, INF, 10, INF, INF, 10, 0} //23
        };

        // 경로를 저장하는 배열을 가중치 값과 동일하게 초기화
        for(int rowIndex = 0; rowIndex < nodeCount; rowIndex++) {
            for(int columnIndex = 0; columnIndex < nodeCount; columnIndex++) {
                distanceArray[rowIndex][columnIndex] = weightArray[rowIndex][columnIndex];
            }
        }
    }

    // Floyd 알고리즘 핵심 알고리즘. 가중치를 계산하여 최단 경로 알고리즘에 합한 값을 저장.
    // 경유지 배열에는 조건문에 맞는 조건의 점을 저장(해당 점으로 가기 직전의 경유지를 저장)
    public void floyd() {

        for(int betweenIndex = 0; betweenIndex < nodeCount; betweenIndex++) {
            for(int startIndex = 0; startIndex < nodeCount; startIndex++) {
                for(int endIndex = 0; endIndex < nodeCount; endIndex++) {
                    if(distanceArray[startIndex][endIndex] > distanceArray[startIndex][betweenIndex] + distanceArray[betweenIndex][endIndex]) {
                        distanceArray[startIndex][endIndex] = distanceArray[startIndex][betweenIndex] + distanceArray[betweenIndex][endIndex];
                        pathArray[startIndex][endIndex] = betweenIndex;
                    }
                }
            }
        }
    }
}
