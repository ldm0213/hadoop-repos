package com.hadoop.mapreduce.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** 用户action信息 **/
public class UserActionInfo implements Writable {
    private String userId;
    private String itemId;
    private String categoryId;
    private String behavior;

    public UserActionInfo() {
    }

    public UserActionInfo(String userId, String itemId, String categoryId, String behavior) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.userId);
        dataOutput.writeUTF(this.itemId);
        dataOutput.writeUTF(this.categoryId);
        dataOutput.writeUTF(this.behavior);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.userId = dataInput.readUTF();
        this.itemId = dataInput.readUTF();
        this.categoryId = dataInput.readUTF();
        this.behavior = dataInput.readUTF();
    }
}
