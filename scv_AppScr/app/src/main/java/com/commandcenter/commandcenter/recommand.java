package com.commandcenter.commandcenter;
/**
 * Created by 준형 on 2017-05-10.
 */
import com.amazonaws.mobileconnectors.dynamodbv2.dynamodbmapper.*;

@DynamoDBTable(tableName = "recommand")
public class recommand {
    private String category;
    private String asin;

    @DynamoDBHashKey(attributeName = "category")
    public String getCategory() {
        return category;
    }
    public void setCategory(String category) {
        this.category = category;
    }


    @DynamoDBAttribute(attributeName = "asin")
    public String getAsin(){ return asin;}
    public void setAsin(String asin){this.asin=asin;}

}
