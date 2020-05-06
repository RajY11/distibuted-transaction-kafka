package com.javapath.orderservice.models;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;


public class Item {

    private int itemid;
    private String name;
    private long price;

    public Item(int itemid, String name, long price) {
        this.itemid = itemid;
        this.name = name;
        this.price = price;
    }

    public Item() {
    }

    public int getItemid() {
        return itemid;
    }

    public void setItemid(int itemid) {
        this.itemid = itemid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }
}
