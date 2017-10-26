package com.commandcenter.commandcenter;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseExpandableListAdapter;
import android.widget.RelativeLayout;
import android.widget.TextView;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by kimdonghoon on 2017-05-17.
 */

public class AdpMain extends BaseExpandableListAdapter {
    private Context context;
    private ArrayList<String> arrayGroup;
    private HashMap<String, ArrayList<String>> arrayChild;

    public AdpMain(Context context, ArrayList<String> arrayGroup, HashMap<String, ArrayList<String>> arrayChild) {
        super();
        this.context = context;
        this.arrayGroup = arrayGroup;
        this.arrayChild = arrayChild;
    }

    @Override
    public boolean isChildSelectable (int groupPosition, int childPosition){
        return true;
    }

    @Override
    public boolean hasStableIds (){
        return true;
    }

    @Override
    public int getGroupCount(){
        return arrayGroup.size();
    }

    @Override
    public int getChildrenCount(int groupPosition){
        return arrayChild.get(arrayGroup.get(groupPosition)).size();
    }

    @Override
    public Object getGroup(int groupPosition){
        return arrayGroup.get(groupPosition);
    }

    @Override
    public Object getChild(int groupPosition, int childPosition){
        return arrayChild.get(arrayGroup.get(groupPosition)).get(childPosition);
    }

    @Override
    public long getGroupId(int groupPosition){
        return groupPosition;
    }

    @Override
    public long getChildId(int groupPosition, int childPosition){
        return childPosition;
    }
    // 리스트에 넣어줄 뷰를 리턴할 함수를 정해준다.
    @Override
    public View getGroupView(int groupPosition, boolean isExpanded, View convertView, ViewGroup parent){
        String groupName = arrayGroup.get(groupPosition);
        View v = convertView;

        if(v==null){
            LayoutInflater inflater = (LayoutInflater) context.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
            v=(RelativeLayout)inflater.inflate(R.layout.lvi_group, null);
        }
        TextView textGroup = (TextView) v.findViewById(R.id.textGroup);
        textGroup.setText(groupName);

        return v;
    }
    public View getChildView(int groupPosition, int childPosition, boolean isLastChild, View convertView, ViewGroup parent){
        String childName = arrayChild.get(arrayGroup.get(groupPosition)).get(childPosition);
        View v = convertView;

        if(v==null){
            LayoutInflater inflater = (LayoutInflater) context.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
            v=(RelativeLayout)inflater.inflate(R.layout.lvi_child, null);
        }
        TextView textChild = (TextView) v.findViewById(R.id.textChild);
        textChild.setText(childName);

        return v;
    }
}
