package com.commandcenter.commandcenter;

import android.content.Context;
import android.os.AsyncTask;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import static com.commandcenter.commandcenter.MainActivity.member2;
import static com.commandcenter.commandcenter.global.list_flag;
import static com.commandcenter.commandcenter.global.total_num2;

/**
 * Created by kimdonghoon on 2017-05-17.
 */

public class phpDown_list extends AsyncTask<String, Integer, String> {
    private Context mContext;

    phpDown_list(){
    }
    phpDown_list (Context context){
        mContext=context;
    }
    @Override
    protected String doInBackground(String... urls) {
        String value;
        StringBuilder jsonHtml = new StringBuilder();
        try {
            // 연결 url 설정
            URL url = new URL(urls[0]);
            // 커넥션 객체 생성
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            // 연결되었으면.
            if (conn != null) {
                conn.setConnectTimeout(10000);
                conn.setUseCaches(false);
                // 연결되었음 코드가 리턴되면.
                if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
                    String line;
                    int index;
                    int index2;
                    for (int i=0;;i++ ) {
                        if (i==0){
                            line = br.readLine();
                            index = line.indexOf(":");
                            index2 = line.indexOf(" ");
                            total_num2 = Integer.parseInt(line.substring(index+1, index2)); // 리스트 총 갯수 세기 (string -> int형)
                        }
                        if(list_flag == true) {
                            if (total_num2 == 0) break; // 데이터가 하나도없으면 루프 종료
                            line = br.readLine();

                            // category 읽기
                            index = line.indexOf(":"); // category : 에서 ":"의 index
                            index2 = line.indexOf(",\""); // "category":cookie, 에서 ","의 index
                            value = line.substring(index+1, index2);
                            member2[i].setCategory(value); // member2[i]에 category 넣고

                            // 물품목록 name 읽기
                            line = line.substring(index2 + 1); // , 부터 다시 남은 부분 자르기
                            index = line.indexOf(":"); // 위랑 같이 name의 ":" 부분
                            index2 = line.indexOf(",\""); // 위랑 같이 ","부분
                            value = line.substring(index+1, index2);
                            member2[i].setName_list(value); // member2[i]에 name 넣고

                            // 해당 물품의 asin 읽기
                            line = line.substring(index2 + 1); // , 부터 다시 남은 부분 자르기
                            index = line.indexOf(":"); // 위랑 같이 asin의 ":" 부분
                            index2 = line.indexOf("}"); // 마지막은 }로 닫혀지는 그 index 부분
                            value = line.substring(index+1, index2);
                            member2[i].setAsin(value); // member2[i]에 asin 넣는다.


                            if (i >= total_num2 - 1) // 마지막 데이터라면
                            {
                                break; // 마지막이니 무한루프 종료
                            }
                        }
                        else{ // 처음에는 라인 읽을 필요 X
                            list_flag = true;
                            break;
                        }
                    }
                    br.close();
                }
                conn.disconnect();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return jsonHtml.toString();
    } //doInBackground
}//phpDown