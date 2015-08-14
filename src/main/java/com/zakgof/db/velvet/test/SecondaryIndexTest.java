package com.zakgof.db.velvet.test;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.annotation.AutoKeyed;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.entity.impl.Entities;
import com.zakgof.db.velvet.api.query.IIndexQuery;
import com.zakgof.db.velvet.api.query.IndexQueryFactory;

public class SecondaryIndexTest {

  private IVelvet velvet;
  private IKeyIndexLink<Integer> indexLink;
  private IEntityDef<String, T1> T1ENTITY = Entities.create(T1.class); 
  
  public SecondaryIndexTest() {
    
    velvet = VelvetTestSuite.velvetProvider.get();
    
    String[] name = new String[] {"0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9"};    
    
    indexLink = velvet.<Integer, String, Long>index("node1", "edge", String.class, "child", node -> (long)(int)(node.charAt(node.length() - 1) - '0'));
    
    velvet.put("main", "rootKey", "node1");
    for (int i=0; i<name.length; i++) {
      velvet.put("child", i, name[i]);
      indexLink.connect(i);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testMixedKvs() {
    
    for (int d=0; d<15000; d++) {    
      T1 t1 = new T1("k" + d, d);
      T1ENTITY.put(velvet, t1);
      // System.out.println(d);
    }
    T1ENTITY.put(velvet, new T1("final", -1.0f));
    
    List<T1> list = T1ENTITY.getAll(velvet);
    
    T1 t3_r = T1ENTITY.get(velvet, "k1");
    T1 t5_r = T1ENTITY.get(velvet, "final");
    
    // raw.dumpIndex(T1ENTITY.getKeyClass(), "@n/t1");
  }

  @Test
  public void testGreaterOrEq() {
    check(IndexQueryFactory.greaterOrEqS(-1L),     "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greaterOrEqS(0L),      "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greaterOrEqS(5L),      "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greaterOrEqS(7L),      "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greaterOrEqS(9L),      "a9", "b9");
    check(IndexQueryFactory.greaterOrEqS(10L)      );
  }
  
  @Test
  public void testEqualsTo() {
    check(IndexQueryFactory.equalsToS(4L));    
    check(IndexQueryFactory.equalsToS(6L));
    check(IndexQueryFactory.equalsToS(7L),         "a7", "b7");
    check(IndexQueryFactory.equalsToS(9L),         "a9", "b9");
    check(IndexQueryFactory.equalsToS(0L),         "0");
    check(IndexQueryFactory.equalsToS(10L));
  }
 
  @Test
  public void testGreater() {
    check(IndexQueryFactory.greaterS(-1L),         "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greaterS(0L),          "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greaterS(4L),          "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greaterS(5L),          "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greaterS(7L),          "a9", "b9");
    check(IndexQueryFactory.greaterS(9L));
    check(IndexQueryFactory.greaterS(10L));
  }
  
  @Test
  public void testFirstLast() {
    check(IndexQueryFactory.first(),              "0");
    check(IndexQueryFactory.last(),               "b9");
  }
   
  @Test
  public void testPrev() {
    // check(IndexQueryFactory.prevKey(99));
    check(IndexQueryFactory.prev(0));
    check(IndexQueryFactory.prev(1),           "0");
    check(IndexQueryFactory.prev(2),           "1");
    check(IndexQueryFactory.prev(5),           "c5");
    check(IndexQueryFactory.prev(8),           "a9");
    // check(IndexQueryFactory.prevKey(9));
    // check(IndexQueryFactory.prevKey(10));
  }
  
  @Test
  public void testLess() {
    check(IndexQueryFactory.lessS(-1L));
    check(IndexQueryFactory.lessS(0L));
    check(IndexQueryFactory.lessS(2L),             "0", "1");
    check(IndexQueryFactory.lessS(5L),             "0", "1");
    check(IndexQueryFactory.lessS(9L),             "0", "1", "a5", "b5", "c5", "a7", "b7");    
    check(IndexQueryFactory.lessS(10L),            "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
  }
  
  @Test
  public void testLessOrEq() {
    check(IndexQueryFactory.lessOrEqS(-1L));
    check(IndexQueryFactory.lessOrEqS(0L),         "0");
    check(IndexQueryFactory.lessOrEqS(5L),         "0", "1", "a5", "b5", "c5");
    check(IndexQueryFactory.lessOrEqS(8L),         "0", "1", "a5", "b5", "c5", "a7", "b7");    
    check(IndexQueryFactory.lessOrEqS(9L),         "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.lessOrEqS(10L),        "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
  }
  
  @Test
  public void testRange() {
    check(IndexQueryFactory.rangeS(-1L, true, 10L, true),      "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.rangeS(-1L, false, 10L, false),    "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.rangeS(0L, false, 10L, false),     "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.rangeS(0L, true, 9L, false),       "0", "1", "a5", "b5", "c5", "a7", "b7");
    check(IndexQueryFactory.rangeS(1L, false, 9L, true),       "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.rangeS(2L, true, 7L, false),       "a5", "b5", "c5");
    check(IndexQueryFactory.rangeS(2L, true, 6L, true),        "a5", "b5", "c5");
    check(IndexQueryFactory.rangeS(5L, true, 6L, false),       "a5", "b5", "c5");
    check(IndexQueryFactory.rangeS(5L, true, 5L, true),        "a5", "b5", "c5");
    check(IndexQueryFactory.rangeS(5L, true, 5L, false));
    check(IndexQueryFactory.rangeS(5L, false, 5L, true));
    check(IndexQueryFactory.rangeS(5L, true, 2L, true));    
  }
  
  
  @Test
  public void testNext1() {
    check(IndexQueryFactory.next(3),         "c5"); // b5 -> c5
  }
  
  
  @Test
  public void testNext2() {
    check(IndexQueryFactory.next(4),            "a7"); // c5 -> a7
  }
  
  @Test
  public void testNextEnd() {
    check(IndexQueryFactory.next(8)                 ); // b9 -> nil
  }
  
  private void check(IIndexQuery query, String...v) {
    String[] vals = indexLink.linkKeys(Integer.class, query).stream().map(key -> velvet.get(String.class, "child", key)).collect(Collectors.toList()).toArray(new String[]{});
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