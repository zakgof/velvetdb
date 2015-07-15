package com.zakgof.db.velvet.kvs;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zakgof.db.sqlkvs.MemKvs;
import com.zakgof.db.velvet.IRawVelvet;
import com.zakgof.db.velvet.IRawVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.annotation.AutoKeyed;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.Velvet;
import com.zakgof.db.velvet.VelvetUtil;
import com.zakgof.db.velvet.api.query.IIndexQuery;
import com.zakgof.db.velvet.api.query.IndexQueryFactory;

public class GenericKvsVelvet3Test {

  private GenericKvsVelvet3 raw;
  private IKeyIndexLink<Integer> indexLink;

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

  @Before
  public void init() {
    String[] name = new String[] {"0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9"};    
    MemKvs kvs = new MemKvs();
    raw = new GenericKvsVelvet3(kvs);
    
    indexLink = raw.<Integer, String, Integer>index("node1", "edge", String.class, "child", node -> (int)(node.charAt(node.length() - 1) - '0'));
    
    raw.put("main", "rootKey", "node1");
    for (int i=0; i<name.length; i++) {
      raw.put("child", i, name[i]);
      indexLink.connect(i);
    }
  }
  
  @Test
  public void testGreaterOrEq() {
    check(raw, indexLink, IndexQueryFactory.greaterOrEq(5),     "a5", "b5", "c5", "a7", "b7", "a9", "b9");
  }
  
  @Test
  public void testEqualsTo() {
    check(raw, indexLink, IndexQueryFactory.equalsTo(7),        "a7", "b7");
  }
  
  @Test
  public void testEqualsToNonExisting() {
    check(raw, indexLink, IndexQueryFactory.equalsTo(8)          );
  }
  
  @Test
  public void testGreater() {
    check(raw, indexLink, IndexQueryFactory.greater(5),         "a7", "b7", "a9", "b9");
  }
  
  @Test
  public void testFirst() {
    check(raw, indexLink, IndexQueryFactory.first(),            "0");
  }
  
  @Test
  public void testLast() {
    check(raw, indexLink, IndexQueryFactory.last(),             "b9");
  }
  
  @Test
  public void testPrev() {
    check(raw, indexLink, IndexQueryFactory.prevKey(8),         "a9"); // b9 -> a9
  }
  
  @Test
  public void testPrevNil() {
    check(raw, indexLink, IndexQueryFactory.prevKey(0)          ); // 0 -> nil
  }
  
  @Test
  public void testLess() {
    check(raw, indexLink, IndexQueryFactory.less(5),            "0", "1");
  }
  
  @Test
  public void testLessOrEq() {
    check(raw, indexLink, IndexQueryFactory.lessOrEq(5),        "0", "1", "a5", "b5", "c5");
  }
  
  
  @Test
  public void testNext1() {
    check(raw, indexLink, IndexQueryFactory.nextKey(3),         "c5"); // b5 -> c5
  }
  
  
  @Test
  public void testNext2() {
    check(raw, indexLink, IndexQueryFactory.nextKey(4),            "a7"); // c5 -> a7
  }
  
  @Test
  public void testNextEnd() {
    check(raw, indexLink, IndexQueryFactory.nextKey(8)                 ); // b9 -> nil
  }
  
  private void check(IRawVelvet raw, IKeyIndexLink<Integer> indexLink, IIndexQuery<Integer> query, String...v) {
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
