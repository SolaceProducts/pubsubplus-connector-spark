package com.solace.connector.spark;

import com.google.gson.Gson;
import org.apache.spark.sql.connector.read.streaming.Offset;

public class SolaceOffset extends Offset {
    private String offset;

    public SolaceOffset(String offset){
        this.offset = offset;
    }

    @Override
    public String json() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }
}
