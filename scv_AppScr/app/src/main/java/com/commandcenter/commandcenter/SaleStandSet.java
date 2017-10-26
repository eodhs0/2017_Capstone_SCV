package com.commandcenter.commandcenter;

/**
 * Created by Seongsoo on 2017-05-23.
 */

import android.content.Context;
import android.support.v7.app.AppCompatActivity;
import android.widget.TextView;

public class SaleStandSet {

    // SaleStand 클래스를 배열형태로 가지는 클래스.
    // 이 클래스는 배열 안의 Index와 View, Category를 맵핑하여 Textview의 Location을 세팅한다.
    SaleStand[] saleStands = new SaleStand[24];

    public SaleStandSet(Context context) {

        // TextView의 ID값을 가져온다.
        TextView enterance = (TextView) ((MainActivity)context).findViewById(R.id.enterance);
        TextView videogameLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.videoGameLoad);
        TextView toysLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.toysLoad);
        TextView electronicsLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.electronicsLoad);
        TextView bottomLeftLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.bottomLeftLoad);
        TextView healthLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.healthLoad);
        TextView booksLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.booksLoad);
        TextView bottomMiddleLeftLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.bottomMiddleLeftLoad);
        TextView clothLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.clothLoad);
        TextView officeLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.officeLoad);
        TextView bottomMiddleRightLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.bottomMiddleRightLoad);
        TextView sportsLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.sportsLoad);
        TextView blankTopLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.blackTopLoad);
        TextView bottomRightLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.bottomRightLoad);
        TextView babyLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.babyLoad);
        TextView cdLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.cdLoad);
        TextView foodLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.foodLoad);
        TextView petLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.petLoad);
        TextView toolLoad = (TextView) ((AppCompatActivity)context).findViewById(R.id.toolLoad);
        TextView cacher = (TextView) ((AppCompatActivity)context).findViewById(R.id.cacher);
        TextView notUsedLeft = (TextView) ((AppCompatActivity)context).findViewById(R.id.notUsedLeft);
        TextView notUsedMiddleLeft = (TextView) ((AppCompatActivity)context).findViewById(R.id.notUsedMiddleLeft);
        TextView notUsedMiddleRight = (TextView) ((AppCompatActivity)context).findViewById(R.id.notUsedMiddleRight);
        TextView notUsedRight = (TextView) ((AppCompatActivity)context).findViewById(R.id.notUsedRight);

        for(int i =0; i<24; i++)
            saleStands[i] = new SaleStand();

        // 각 인덱스에 0~23까지 TextView, 카테고리를 맵핑. Location을 받아온다.
        saleStands[0].setSaleCategory("Enterance");
        enterance.getLocationOnScreen(saleStands[0].saleStandLocation);
        saleStands[0].saleStandLocation[0] += (enterance.getWidth()/2);
        saleStands[0].saleStandLocation[1] += (enterance.getHeight()/2);

        saleStands[1].setSaleCategory("Video Games");
        videogameLoad.getLocationOnScreen(saleStands[1].saleStandLocation);
        saleStands[1].saleStandLocation[0] += (videogameLoad.getWidth()/2);
        saleStands[1].saleStandLocation[1] += (videogameLoad.getHeight()/2);

        saleStands[2].setSaleCategory("Toys and Games");
        toysLoad.getLocationOnScreen(saleStands[2].saleStandLocation);
        saleStands[2].saleStandLocation[0] += (toysLoad.getWidth()/2);
        saleStands[2].saleStandLocation[1] += (toysLoad.getHeight()/2);

        saleStands[3].setSaleCategory("Electronics");
        electronicsLoad.getLocationOnScreen(saleStands[3].saleStandLocation);
        saleStands[3].saleStandLocation[0] += (electronicsLoad.getWidth()/2);

        saleStands[4].setSaleCategory("Not Used Load Left");
        notUsedLeft.getLocationOnScreen(saleStands[4].saleStandLocation);
        saleStands[4].saleStandLocation[0] += (notUsedLeft.getWidth()/2);
        saleStands[4].saleStandLocation[1] += (notUsedLeft.getHeight()/2);

        saleStands[5].setSaleCategory("Bottom Left Load");
        bottomLeftLoad.getLocationOnScreen(saleStands[5].saleStandLocation);
        saleStands[5].saleStandLocation[0] += (bottomLeftLoad.getWidth()/2);
        saleStands[5].saleStandLocation[1] += (bottomLeftLoad.getHeight()/2);

        saleStands[6].setSaleCategory("Health and Personal Care");
        healthLoad.getLocationOnScreen(saleStands[6].saleStandLocation);
        saleStands[6].saleStandLocation[0] += (healthLoad.getWidth()/2);
        saleStands[6].saleStandLocation[1] += (healthLoad.getHeight()/2);

        saleStands[7].setSaleCategory("Books");
        booksLoad.getLocationOnScreen(saleStands[7].saleStandLocation);
        saleStands[7].saleStandLocation[0] += (booksLoad.getWidth()/2);

        saleStands[8].setSaleCategory("Not Used Load Middle Left");
        notUsedMiddleLeft.getLocationOnScreen(saleStands[8].saleStandLocation);
        saleStands[8].saleStandLocation[0] += (notUsedMiddleLeft.getWidth()/2);
        saleStands[8].saleStandLocation[1] += (notUsedMiddleLeft.getHeight()/2);

        saleStands[9].setSaleCategory("Bottom Middle Left Load");
        bottomMiddleLeftLoad.getLocationOnScreen(saleStands[9].saleStandLocation);
        saleStands[9].saleStandLocation[0] += (bottomMiddleLeftLoad.getWidth()/2);
        saleStands[9].saleStandLocation[1] += (bottomMiddleLeftLoad.getHeight()/2);

        saleStands[10].setSaleCategory("Clothing, Shoes and Jewelry");
        clothLoad.getLocationOnScreen(saleStands[10].saleStandLocation);
        saleStands[10].saleStandLocation[0] += (clothLoad.getWidth()/2);
        saleStands[10].saleStandLocation[1] += (clothLoad.getHeight()/2);

        saleStands[11].setSaleCategory("Office Products");
        officeLoad.getLocationOnScreen(saleStands[11].saleStandLocation);
        saleStands[11].saleStandLocation[0] += (officeLoad.getWidth()/2);

        saleStands[12].setSaleCategory("Not Used Load Middle Right");
        notUsedMiddleRight.getLocationOnScreen(saleStands[12].saleStandLocation);
        saleStands[12].saleStandLocation[0] += (notUsedRight.getWidth()/2);
        saleStands[12].saleStandLocation[1] += (notUsedRight.getHeight()/2);

        saleStands[13].setSaleCategory("Bottom Middle Right Load");
        bottomMiddleRightLoad.getLocationOnScreen(saleStands[13].saleStandLocation);
        saleStands[13].saleStandLocation[0] += (bottomMiddleRightLoad.getWidth()/2);
        saleStands[13].saleStandLocation[1] += (bottomMiddleRightLoad.getHeight()/2);

        saleStands[14].setSaleCategory("Sports and Outdoors");
        sportsLoad.getLocationOnScreen(saleStands[14].saleStandLocation);
        saleStands[14].saleStandLocation[0] += (sportsLoad.getWidth()/2);
        saleStands[14].saleStandLocation[1] += (sportsLoad.getHeight()/2);

        saleStands[15].setSaleCategory("Blank Top Load");
        blankTopLoad.getLocationOnScreen(saleStands[15].saleStandLocation);
        saleStands[15].saleStandLocation[0] += (blankTopLoad.getWidth()/2);

        saleStands[16].setSaleCategory("Not Used Load Right");
        notUsedRight.getLocationOnScreen(saleStands[16].saleStandLocation);
        saleStands[16].saleStandLocation[0] += (notUsedRight.getWidth()/2);
        saleStands[16].saleStandLocation[1] += (notUsedRight.getHeight()/2);

        saleStands[17].setSaleCategory("Bottom Right Load");
        bottomRightLoad.getLocationOnScreen(saleStands[17].saleStandLocation);
        saleStands[17].saleStandLocation[0] += (bottomRightLoad.getWidth()/2);
        saleStands[17].saleStandLocation[1] += (bottomRightLoad.getHeight()/2);

        saleStands[18].setSaleCategory("Baby");
        babyLoad.getLocationOnScreen(saleStands[18].saleStandLocation);
        saleStands[18].saleStandLocation[0] += (babyLoad.getWidth()/2);
        saleStands[18].saleStandLocation[1] += (babyLoad.getHeight()/2);

        saleStands[19].setSaleCategory("CDs and Vinyl");
        cdLoad.getLocationOnScreen(saleStands[19].saleStandLocation);
        saleStands[19].saleStandLocation[0] += (cdLoad.getWidth()/2);

        saleStands[20].setSaleCategory("Casher");
        cacher.getLocationOnScreen(saleStands[20].saleStandLocation);
        saleStands[20].saleStandLocation[0] += (cacher.getWidth()/2);
        saleStands[20].saleStandLocation[1] += (cacher.getHeight()/2);

        saleStands[21].setSaleCategory("Grocery and Gourmet Food");
        foodLoad.getLocationOnScreen(saleStands[21].saleStandLocation);
        saleStands[21].saleStandLocation[0] += (foodLoad.getWidth()/2);
        saleStands[21].saleStandLocation[1] += (foodLoad.getHeight()/2);

        saleStands[22].setSaleCategory("Pet Supplies");
        petLoad.getLocationOnScreen(saleStands[22].saleStandLocation);
        saleStands[22].saleStandLocation[0] += (petLoad.getWidth()/2);
        saleStands[22].saleStandLocation[1] += (petLoad.getHeight()/2);

        saleStands[23].setSaleCategory("Tools & Home Improvement");
        toolLoad.getLocationOnScreen(saleStands[23].saleStandLocation);
        saleStands[23].saleStandLocation[0] += (toolLoad.getWidth()/2);

    }
}
