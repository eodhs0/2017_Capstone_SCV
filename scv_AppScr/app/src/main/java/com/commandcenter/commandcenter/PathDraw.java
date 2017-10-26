package com.commandcenter.commandcenter;

/**
 * Created by Seongsoo on 2017-05-23.
 */
import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Path;
import android.graphics.Rect;
import android.view.View;

public class PathDraw extends View {

    // 추적한 경로들을 저장한 배열을 바탕으로 맵에 경로를 그려준다.

    int[] saveStandArray;
    SaleStandSet saveSaleStandSet;

    public PathDraw(Context context) {
        super(context);
    }

    public void makePath(int[] standArray, SaleStandSet saleStandSet) {
        saveStandArray = standArray;
        saveSaleStandSet = saleStandSet;
    }

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);

        System.out.printf("RECALL TEST : %d\n", saveStandArray.length);

        Bitmap bitmap = BitmapFactory.decodeResource(getResources(), R.drawable.map);
        Rect rtDest = new Rect(0, 0, getWidth(), getHeight());
        canvas.drawBitmap(bitmap, null, rtDest, null);

        Paint paint = new Paint();
        paint.setColor(Color.BLACK);
        paint.setStrokeWidth(5f);
        paint.setStyle(Paint.Style.STROKE);

        Path path = new Path();
        path.moveTo(saveSaleStandSet.saleStands[saveStandArray[0]].saleStandLocation[0], saveSaleStandSet.saleStands[saveStandArray[0]].saleStandLocation[1]);
        path.lineTo(saveSaleStandSet.saleStands[saveStandArray[0]].saleStandLocation[0], saveSaleStandSet.saleStands[saveStandArray[0]].saleStandLocation[1]);

        for (int i = 1; i < saveStandArray.length; i++) {
            if (saveStandArray[i] != 0) {
                path.lineTo(saveSaleStandSet.saleStands[saveStandArray[i]].saleStandLocation[0], saveSaleStandSet.saleStands[saveStandArray[i]].saleStandLocation[1]);
            }
        }

        canvas.drawPath(path, paint);
    }
}
