package com.zakgof.db.velvet.test;

import java.io.Serializable;

public class TestEnt3 implements Serializable {

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + key;
        result = prime * result + ((str == null) ? 0 : str.hashCode());
        result = prime * result + (int) (weight ^ (weight >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TestEnt3 other = (TestEnt3) obj;
        if (key != other.key)
            return false;
        if (str == null) {
            if (other.str != null)
                return false;
        } else if (!str.equals(other.str))
            return false;
        if (weight != other.weight)
            return false;
        return true;
    }

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

    public void setWeightAndStr(long weight, String str) {
        this.weight = weight;
        this.str = str;
    }
}