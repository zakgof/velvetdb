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

public class VelvetTest {

  private IVelvet velvet;
  private IKeyIndexLink<Integer> indexLink;
  private IEntityDef<String, T1> T1ENTITY = Entities.create(T1.class); 
  
  public VelvetTest() {
    
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
    check(IndexQueryFactory.greaterOrEq(-1L),     "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greaterOrEq(0L),      "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greaterOrEq(5L),      "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greaterOrEq(7L),      "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greaterOrEq(9L),      "a9", "b9");
    check(IndexQueryFactory.greaterOrEq(10L)      );
  }
  
  @Test
  public void testEqualsTo() {
    check(IndexQueryFactory.equalsTo(4L));    
    check(IndexQueryFactory.equalsTo(6L));
    check(IndexQueryFactory.equalsTo(7L),         "a7", "b7");
    check(IndexQueryFactory.equalsTo(9L),         "a9", "b9");
    check(IndexQueryFactory.equalsTo(0L),         "0");
    check(IndexQueryFactory.equalsTo(10L));
  }
 
  @Test
  public void testGreater() {
    check(IndexQueryFactory.greater(-1L),         "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greater(0L),          "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greater(4L),          "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greater(5L),          "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.greater(7L),          "a9", "b9");
    check(IndexQueryFactory.greater(9L));
    check(IndexQueryFactory.greater(10L));
  }
  
  @Test
  public void testFirstLast() {
    check(IndexQueryFactory.first(),              "0");
    check(IndexQueryFactory.last(),               "b9");
  }
   
  @Test
  public void testPrev() {
    // check(IndexQueryFactory.prevKey(99));
    check(IndexQueryFactory.prevKey(0));
    check(IndexQueryFactory.prevKey(1),           "0");
    check(IndexQueryFactory.prevKey(2),           "1");
    check(IndexQueryFactory.prevKey(5),           "c5");
    check(IndexQueryFactory.prevKey(8),           "a9");
    // check(IndexQueryFactory.prevKey(9));
    // check(IndexQueryFactory.prevKey(10));
  }
  
  @Test
  public void testLess() {
    check(IndexQueryFactory.less(-1L));
    check(IndexQueryFactory.less(0L));
    check(IndexQueryFactory.less(2L),             "0", "1");
    check(IndexQueryFactory.less(5L),             "0", "1");
    check(IndexQueryFactory.less(9L),             "0", "1", "a5", "b5", "c5", "a7", "b7");    
    check(IndexQueryFactory.less(10L),            "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
  }
  
  @Test
  public void testLessOrEq() {
    check(IndexQueryFactory.lessOrEq(-1L));
    check(IndexQueryFactory.lessOrEq(0L),         "0");
    check(IndexQueryFactory.lessOrEq(5L),         "0", "1", "a5", "b5", "c5");
    check(IndexQueryFactory.lessOrEq(8L),         "0", "1", "a5", "b5", "c5", "a7", "b7");    
    check(IndexQueryFactory.lessOrEq(9L),         "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.lessOrEq(10L),        "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
  }
  
  @Test
  public void testRange() {
    check(IndexQueryFactory.range(-1L, true, 10L, true),      "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.range(-1L, false, 10L, false),    "0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.range(0L, false, 10L, false),     "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.range(0L, true, 9L, false),       "0", "1", "a5", "b5", "c5", "a7", "b7");
    check(IndexQueryFactory.range(1L, false, 9L, true),       "a5", "b5", "c5", "a7", "b7", "a9", "b9");
    check(IndexQueryFactory.range(2L, true, 7L, false),       "a5", "b5", "c5");
    check(IndexQueryFactory.range(2L, true, 6L, true),        "a5", "b5", "c5");
    check(IndexQueryFactory.range(5L, true, 6L, false),       "a5", "b5", "c5");
    check(IndexQueryFactory.range(5L, true, 5L, true),        "a5", "b5", "c5");
    check(IndexQueryFactory.range(5L, true, 5L, false));
    check(IndexQueryFactory.range(5L, false, 5L, true));
    check(IndexQueryFactory.range(5L, true, 2L, true));    
  }
  
  
  @Test
  public void testNext1() {
    check(IndexQueryFactory.nextKey(3),         "c5"); // b5 -> c5
  }
  
  
  @Test
  public void testNext2() {
    check(IndexQueryFactory.nextKey(4),            "a7"); // c5 -> a7
  }
  
  @Test
  public void testNextEnd() {
    check(IndexQueryFactory.nextKey(8)                 ); // b9 -> nil
  }
  
  private void check(IIndexQuery<Integer> query, String...v) {
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
