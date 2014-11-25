package com.zakgof.db.velvet.kvs;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.zakgof.db.sqlkvs.MemKvs;
import com.zakgof.db.velvet.AutoKeyed;
import com.zakgof.db.velvet.IRawVelvet;
import com.zakgof.db.velvet.IRawVelvet.ISortedIndexLink;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IndexQuery;
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

  @Test
  public void testArrayIndex() {

    String[] name = new String[] {"0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9"};    
    MemKvs kvs = new MemKvs();
    GenericKvsVelvet3 raw = new GenericKvsVelvet3(kvs);
    
    ISortedIndexLink<Integer, String, Integer> indexLink = raw.<Integer, String, Integer>index("node1", "edge", String.class, "child", node -> (int)(node.charAt(node.length() - 1) - '0'));
    
    raw.put("main", "rootKey", "node1");
    for (int i=0; i<name.length; i++) {
      raw.put("child", i, name[i]);
      indexLink.connect(i);
    }
        
    
//    check(raw, indexLink, IndexQuery.<String, Integer>greaterOrEq(5),     "a5", "b5", "c5", "a7", "b7", "a9", "b9");
//    check(raw, indexLink, IndexQuery.<String, Integer>equalsTo(7),        "a7", "b7");
//    check(raw, indexLink, IndexQuery.<String, Integer>equalsTo(8)          );
//    check(raw, indexLink, IndexQuery.<String, Integer>greater(5),         "a7", "b7", "a9", "b9");
    check(raw, indexLink, IndexQuery.<String, Integer>last(),             "b9");
    check(raw, indexLink, IndexQuery.<String, Integer>less(5),            "0", "1");
    check(raw, indexLink, IndexQuery.<String, Integer>lessOrEq(5),        "0", "1", "a5", "b5", "c5");
    check(raw, indexLink, IndexQuery.<String, Integer>next("b5"),         "c5");
    check(raw, indexLink, IndexQuery.<String, Integer>next("c5"),         "a7");
    check(raw, indexLink, IndexQuery.<String, Integer>next("b9")           );
    
    
  }
  
  private void check(IRawVelvet raw, ISortedIndexLink<Integer, String, Integer> indexLink, IndexQuery<String, Integer> query, String...v) {
    String[] vals = indexLink.linkKeys(Integer.class, query).stream().map(key -> raw.get(String.class, "child", key)).collect(Collectors.toList()).toArray(new String[]{});
    Assert.assertArrayEquals(v, vals);
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
