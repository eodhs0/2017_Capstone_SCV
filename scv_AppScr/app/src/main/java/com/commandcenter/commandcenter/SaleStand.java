package com.commandcenter.commandcenter;

/**
 * Created by Seongsoo on 2017-05-23.
 */
public class SaleStand {

    // 판매대 클래스. 상품 카테고리의 이름을 맵핑시키는 saleCategory,
    // 해당 상품 진열대의 위치와 연결되는 길의 Location을 저장할 수 있는 배열을 가진다.

    private String saleCategory;
    public int[] saleStandLocation;

    public SaleStand() {
        saleStandLocation = new int[2];
    }

    public String getSaleCategory() {
        return saleCategory;
    }

    public void setSaleCategory(String saleCategory) {
        this.saleCategory = saleCategory;
    }

}
