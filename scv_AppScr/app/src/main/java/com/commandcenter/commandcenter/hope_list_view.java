package com.commandcenter.commandcenter;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.util.SparseBooleanArray;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.ExpandableListView;
import android.widget.ListView;

import java.util.ArrayList;
import java.util.HashMap;

import static com.commandcenter.commandcenter.MainActivity.member2;
import static com.commandcenter.commandcenter.global.item;
import static com.commandcenter.commandcenter.global.total_num2;

/**
 * Created by kimdonghoon on 2017-05-17.
 */

public class hope_list_view extends AppCompatActivity {
    ExpandableListView listMain;
    // arrayGroup 이라는 배열리스트 생성 (상위 리스트의 제목)
    private ArrayList<String> arrayGroup = new ArrayList<String>();
    // 해쉬맵을 생성 범주별 세부 내용이 들어간다. 왼쪽은 부모, 오른쪽은 child
    private HashMap<String, ArrayList<String>> arrayChild = new HashMap<String, ArrayList<String>>();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_hope_list_view);

        // ArrayAdapter 생성. 아이템 View를 선택(multiple choice)가능하도록 만듦.
        final ArrayAdapter adapter = new ArrayAdapter(this, android.R.layout.simple_list_item_multiple_choice, item) ;
        final ListView listview = (ListView) findViewById(R.id.listview1) ;
        listview.setAdapter(adapter) ;

        listMain = (ExpandableListView) this.findViewById(R.id.expandableListView1);
        setArrayData();

        // 아래는 아마존에서 가져온 물품 리스트의 전부
        listMain.setAdapter(new AdpMain(this, arrayGroup, arrayChild));
        // 구매희망 리스트 클릭 할 때
        listMain.setOnChildClickListener(new ExpandableListView.OnChildClickListener() {
            @Override
            public boolean onChildClick(ExpandableListView parent, View v, int groupPosition, int childPosition, long id) {
                String menu = arrayChild.get(arrayGroup.get(groupPosition)).get(childPosition);
                // 아이템 추가.
                item.add(menu);
                // listview 갱신
                adapter.notifyDataSetChanged();
                return false;
            }
        });
        // 여기는 구매자가 희망리스트를 작성한 리스트 (삭제가능)
        listview.setAdapter(adapter) ;
        Button deleteButton = (Button)findViewById(R.id.delete);
        deleteButton.setOnClickListener(new Button.OnClickListener() {
            public void onClick(View v){
                SparseBooleanArray checkedItems = listview.getCheckedItemPositions();
                int count = adapter.getCount();
                for(int i=count-1; i>=0; i--){
                    if(checkedItems.get(i)){
                        item.remove(i);
                    }
                }
                // 모든 선택 상태 초기화
                listview.clearChoices();
                adapter.notifyDataSetChanged();
            }
        });

        // selectAll button에 대한 이벤트 처리.
        Button selectAllButton = (Button)findViewById(R.id.selectAll);
        selectAllButton.setOnClickListener(new Button.OnClickListener(){
            public void onClick(View v){
                int count = 0;
                count = adapter.getCount();
                for(int i=0; i<count; i++){
                    listview.setItemChecked(i, true);
                }
            }
        });
        //돌아가기 버튼 클릭 했을 때
        Button search = (Button)findViewById(R.id.back_main);
        search.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View view) {
                global.changemsg = 1;
                finish();
            }
        });
    }
    private void setArrayData() {
        boolean flag; // 카테고리 중복 되어 있지 않다고 가정
        for(int i=0; i<total_num2; i++){
            flag = false;
            if(i == 0) {
                arrayGroup.add(member2[i].getCategory());
                continue;
            }
            for(int j=0; j<=arrayGroup.size()-1; j++){
                if(member2[i].getCategory().equals(arrayGroup.get(j).toString())){
                    flag = true;
                    break;
                }
            }
            if(flag == false){
                arrayGroup.add(member2[i].getCategory());
            }
        }

        // category 13개
        ArrayList<String> Eletronics = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Eletronics")) {
                Eletronics.add(member2[i].getName_list());
            }
        }

        ArrayList<String> Toys_and_Games = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Toys and Games")) {
                Toys_and_Games.add(member2[i].getName_list());
            }
        }

        ArrayList<String> Video_Games = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Video Games")) {
                Video_Games.add(member2[i].getName_list());
            }
        }

        ArrayList<String> Hearth_and_Personal_Care = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Hearth and Personal Care")) {
                Hearth_and_Personal_Care.add(member2[i].getName_list());
            }
        }

        ArrayList<String> Clothing_Shoes_and_Jewelry = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Clothing, Shoes and Jewelry")) {
                Clothing_Shoes_and_Jewelry.add(member2[i].getName_list());
            }
        }

        ArrayList<String> Sports_and_Outdoors = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Sports & Outdoors")) {
                Sports_and_Outdoors.add(member2[i].getName_list());
            }
        }

        ArrayList<String> Baby = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Baby")) {
                Baby.add(member2[i].getName_list());
            }
        }

        ArrayList<String> Tools_and_Home_Improvement = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Tools & Home Improvement")) {
                Tools_and_Home_Improvement.add(member2[i].getName_list());
            }
        }

        ArrayList<String> Pet_Supplies = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Pet Supplies")) {
                Pet_Supplies.add(member2[i].getName_list());
            }
        }

        ArrayList<String> Grocery_and_Gourmet_Food = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Grocery & Gourmet Food")) {
                Grocery_and_Gourmet_Food.add(member2[i].getName_list());
            }
        }
        /*
        ArrayList<String> Enter = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Enter")) {
                Enter.add(member2[i].getName_list());
            }
        }

        ArrayList<String> Caher = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Caher")) {
                Caher.add(member2[i].getName_list());
            }
        }
        */

        ArrayList<String> Books = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Books")) {
                Books.add(member2[i].getName_list());
            }
        }

        ArrayList<String> OfficeProduct = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("Office Products")) {
                OfficeProduct.add(member2[i].getName_list());
            }
        }

        ArrayList<String> CDs_and_Vinyl = new ArrayList<String>();
        for(int i=0; i<total_num2; i++) {
            if (member2[i].getCategory().equals("CDs and Vinyl")) {
                CDs_and_Vinyl.add(member2[i].getName_list());
            }
        }
        //arrayRamen.add("신라면");


        arrayChild.put(arrayGroup.get(0), Sports_and_Outdoors);
        arrayChild.put(arrayGroup.get(1), Books);
        arrayChild.put(arrayGroup.get(2), Baby);
        arrayChild.put(arrayGroup.get(3), OfficeProduct);
        arrayChild.put(arrayGroup.get(4), Pet_Supplies);
        arrayChild.put(arrayGroup.get(5), Tools_and_Home_Improvement);
        arrayChild.put(arrayGroup.get(6), Video_Games);
        arrayChild.put(arrayGroup.get(7), Hearth_and_Personal_Care);
        arrayChild.put(arrayGroup.get(8), CDs_and_Vinyl);
        arrayChild.put(arrayGroup.get(9), Grocery_and_Gourmet_Food);
        arrayChild.put(arrayGroup.get(10), Eletronics);
        arrayChild.put(arrayGroup.get(11), Toys_and_Games);
        arrayChild.put(arrayGroup.get(12), Clothing_Shoes_and_Jewelry);



    }
}
