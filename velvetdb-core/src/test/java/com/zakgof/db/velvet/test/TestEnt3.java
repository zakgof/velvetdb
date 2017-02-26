package com.zakgof.db.velvet.test;

import java.io.Serializable;

public class TestEnt3 implements Serializable {

    private static final long serialVersionUID = -662154305170322611L;

    private int key;
    private long weight;
    private String str;

    public TestEnt3() {
    }

    public TestEnt3(int key, long weight, String str) {
        this.key = key;
        this.weight = weight;
        this.str = str;
    }

    public int getKey() {
        return key;
    }

    public long getWeight() {
        return weight;
    }

    public String getStr() {
        return str;
    }

    @Override
    public String toString() {
        return key + " " + weight + " " + str;
    }
}