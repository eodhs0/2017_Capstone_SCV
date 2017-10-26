package com.commandcenter.commandcenter;

/**
 * Created by kimdonghoon on 2017-05-21.
 */

public class Member_list {
    private String category;
    private String name_list;
    private String asin_list;

    Member_list() {}
    void setCategory(String category){ this.category = category; }
    String getCategory(){
        return this.category;
    }

    void setName_list(String name_list){
        this.name_list = name_list;
    }
    String getName_list(){ return this.name_list; }

    void setAsin(String asin) { this.asin_list = asin; }
    String getAsin(){
        return this.asin_list;
    }
}

