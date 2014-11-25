package com.zakgof.db.velvet.kvs;

import java.util.List;

import org.junit.Test;

import com.zakgof.db.sqlkvs.MemKvs;
import com.zakgof.db.velvet.AutoKeyed;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.Velvet;
import com.zakgof.db.velvet.VelvetUtil;

public class GenericKvsVelvet3Test {

  public static void main(String[] args) {
    new GenericKvsVelvet3Test().testMixedKvs();
  }

  @SuppressWarnings("unused")
  @Test
  public void testMixedKvs() {

    MemKvs kvs = new MemKvs();
    GenericKvsVelvet3 raw = new GenericKvsVelvet3(kvs);
    IVelvet velvet = new Velvet(raw);    
    
    for (int d=0; d<15000; d++) {    
      T1 t1 = new T1("k" + d, d);
      velvet.put(t1);
      System.out.println(d);
    }
    velvet.put(new T1("final", -1.0f));
    
    List<T1> list = velvet.allOf(T1.class);
    
    T1 t3_r = velvet.get(T1.class, "k1");
    T1 t5_r = velvet.get(T1.class, "final");
    
    kvs.dump();
    raw.dumpIndex(VelvetUtil.keyClassOf(T1.class), "@n/t1");
    
  }

  public static class T1 extends AutoKeyed {
    public T1() {
    }

    public T1(String s, float m) {
      this.s = s;
      this.m = m;
    }

    public float getM() {
      return m;
    }

    private float m;
    private String s;

    @Override
    public String toString() {
      return s + " " + m;
    }
  }

  class KK extends AutoKeyed {
  }

}
