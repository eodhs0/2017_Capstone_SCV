package com.commandcenter.commandcenter;
/**
 * Created by 준형 on 2017-05-10.
 */
import com.amazonaws.mobileconnectors.dynamodbv2.dynamodbmapper.*;

@DynamoDBTable(tableName = "Product")
public class Product {
    private String asin;
    private String review1;
    private String name;
    private String category;
    private String relate;
    private String image;

    @DynamoDBHashKey(attributeName = "asin")
    public String getAsin() {
        return asin;
    }
    public void setAsin(String asin) {
        this.asin = asin;
    }

    @DynamoDBAttribute(attributeName = "review1")
    public String getReview1(){ return review1;}
    public void setReview1(String review1){this.review1=review1;}

    @DynamoDBAttribute(attributeName = "name")
    public String getName(){ return name;}
    public void setName(String name){this.name=name;}

    @DynamoDBAttribute(attributeName = "category")
    public String getCategory(){ return category;}
    public void setCategory(String category){this.category=category;}

    @DynamoDBAttribute(attributeName = "relate")
    public String getRelate(){ return relate;}
    public void setRelate(String relate){this.relate=relate;}

    @DynamoDBAttribute(attributeName = "image")
    public String getImage(){ return image;}
    public void setImage(String image){this.image=image;}

}
