package com.facebook.presto.elasticsearch;

// http://stackoverflow.com/questions/26183948/output-list-of-all-paths-to-leaf-nodes-in-a-json-document-in-java

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MyJSONTest {

    /*
    leaf is of the form -> path:obj
     */
    private ArrayList<String> leaves;

    public MyJSONTest()
    {
        leaves = new ArrayList<String>();
    }

    public List<String> getListJson(JSONObject json) throws JSONException {
        listJSONObject("", json);
        return leaves;
    }

    private void listObject(String parent, Object data) throws JSONException {
        if (data instanceof JSONObject) {
            listJSONObject(parent, (JSONObject)data);
        } else if (data instanceof JSONArray) {
            listJSONArray(parent, (JSONArray) data);
        } else {
            listPrimitive(parent, data);
        }
    }

    private void listJSONObject(String parent, JSONObject json) throws JSONException {
        Iterator it = json.keys();
        while (it.hasNext()) {
            String key = (String)it.next();
            Object child = json.get(key);
            String childKey = parent.isEmpty() ? key : parent + "." + key;
            listObject(childKey, child);
        }
    }

    private void listJSONArray(String parent, JSONArray json) throws JSONException {
        for (int i = 0; i < json.length(); i++) {
            Object data = json.get(i);
            listObject(parent, data);
        }
    }

    private void listPrimitive(String parent, Object obj) {
        //System.out.println(parent + ":"  + obj);
        leaves.add(parent + ":" + obj.toString());
    }

    public static void main(String[] args) throws JSONException {
        String data = "{\"store\":{\"book\":[{\"category\":\"reference\",\"author\":\"NigelRees\",\"title\":\"SayingsoftheCentury\",\"price\":8.95},{\"category\":\"fiction\",\"author\":\"HermanMelville\",\"title\":\"MobyDick\",\"isbn\":\"0-553-21311-3\",\"price\":8.99},],\"bicycle\":{\"color\":\"red\",\"price\":19.95}},\"expensive\":10}";
        JSONObject json = new JSONObject(data);
        System.out.println(json.get("store"));
        //System.out.println(json.toString(2));
        List<String> leaves = (new MyJSONTest()).getListJson(json);

        for(String s : leaves)
        {
            System.out.println(s);
            System.out.println(".....");
        }
    }

}