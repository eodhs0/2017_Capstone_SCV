package com.commandcenter.commandcenter;

import android.content.Context;
import android.os.AsyncTask;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import static com.commandcenter.commandcenter.MainActivity.member;
import static com.commandcenter.commandcenter.global.buy_item;
import static com.commandcenter.commandcenter.global.countdb;
import static com.commandcenter.commandcenter.global.dbfirst;
import static com.commandcenter.commandcenter.global.total_num;

/**
 * Created by kimdonghoon on 2017-05-17.
 */

public class phpDown extends AsyncTask<String, Integer, String> {
    private Context mContext;

    phpDown(){
    }
    phpDown (Context context){
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
                    for (int i=0;;i++ ) {
                        String line = br.readLine();
                        if (i==0){
                            int index = line.indexOf(":");
                            int index2 = line.indexOf(" ");
                            countdb = total_num; // countdb 초기화
                            total_num = Integer.parseInt(line.substring(index+1, index2)); // 바코드 총 갯수 세기 (string -> int형)
                            if(dbfirst == false){
                                dbfirst = true;
                                countdb = total_num; // countdb 초기화 (처음 실행시 딱 한번만 실행됨)
                            }
                        }
                        if(total_num == 0) break; // 데이터가 하나도없으면 루프 종료
                        int index = line.indexOf("cnt"); // cnt의 c의 인덱스값 찾기
                        index += 10; // +10 해주면 날짜의 첫번째 수가 나옴
                        value = line.substring(index, index+17); // 날짜를 value에 넣기
                        member[i].setDate(value);
                        // bacord number
                        line = line.substring(index+17);
                        index = line.indexOf(":");
                        try { // 바코드 처음 찍을 때
                            value = line.substring(index + 1, index + 11);
                        } catch(IndexOutOfBoundsException e){ // 바코드 두번째 부터 찍을 때
                            line = br.readLine();
                            value = line.substring(0, 10);
                        }
                        member[i].setBacord(value);
                        // 텍스트 반환하는 리턴형 (날짜, 바코드)

                        if(i >= total_num-1) // 마지막 데이터라면
                        {
                            jsonHtml.append(member[i].getBacord());
                            if(countdb != total_num) // 처음시작이 아니고 바코드 변화값이 있을 때
                            {
                                // 바코드 찍은 갯수만큼 buy_item 어레이 리스트에 저장
                                for(int j=0; j<total_num-countdb; j++) {
                                    //buy_item.add(0, member[i].getBacord()); // 사용자가 보기 쉽게 맨 앞에 저장
                                    buy_item.add(member[i].getBacord());
                                    System.out.println("buyitem : " + buy_item);
                                    global.changemsg2 = 1;
                                }
                            }
                            break; // 마지막이니 무한루프 종료
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