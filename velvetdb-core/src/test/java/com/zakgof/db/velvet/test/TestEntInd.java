package com.zakgof.db.velvet.test;

import java.io.Serializable;

import com.zakgof.db.velvet.annotation.Index;
import com.zakgof.db.velvet.annotation.Key;

public class TestEntInd implements Serializable {

    private static final long serialVersionUID = 237134828293703443L;

    @Index
    private int key;

    private long weight;
    private String str;

    public TestEntInd() {
    }

    public TestEntInd(int key, long weight, String str) {
        this.key = key;
        this.weight = weight;
        this.str = str;
    }

    @Key
    public int getKey() {
        return key;
    }

    @Index(name="w2")
    public long getWeight() {
        return weight;
    }

    @Index
    public String getStr() {
        return str;
    }

    @Override
    public String toString() {
        return key + " " + weight + " " + str;
    }
}