package com.zakgof.db.velvet.test;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.zakgof.db.sqlkvs.MemKvs;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.annotation.AutoKeyed;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.entity.impl.Entities;
import com.zakgof.db.velvet.api.query.IIndexQuery;
import com.zakgof.db.velvet.api.query.IndexQueryFactory;
import com.zakgof.db.velvet.kvs.GenericKvsVelvet3;

public abstract class VelvetTest {

  private IVelvet velvet;
  private IKeyIndexLink<Integer> indexLink;
  private IEntityDef<String, T1> T1ENTITY = Entities.create(T1.class); 

  @SuppressWarnings("unused")
  @Test
  public void testMixedKvs() {

    MemKvs kvs = new MemKvs();
    IVelvet velvet = new GenericKvsVelvet3(kvs);
    
    for (int d=0; d<15000; d++) {    
      T1 t1 = new T1("k" + d, d);
      T1ENTITY.put(velvet, t1);
      // System.out.println(d);
    }
    T1ENTITY.put(velvet, new T1("final", -1.0f));
    
    List<T1> list = T1ENTITY.getAll(velvet);
    
    T1 t3_r = T1ENTITY.get(velvet, "k1");
    T1 t5_r = T1ENTITY.get(velvet, "final");
    
    kvs.dump();
    // raw.dumpIndex(T1ENTITY.getKeyClass(), "@n/t1");
  }

  public VelvetTest() {
   
    String[] name = new String[] {"0", "1", "a5", "b5", "c5", "a7", "b7", "a9", "b9"};    
    velvet = createVelvet();
    
    indexLink = velvet.<Integer, String, Long>index("node1", "edge", String.class, "child", node -> (long)(int)(node.charAt(node.length() - 1) - '0'));
    
    velvet.put("main", "rootKey", "node1");
    for (int i=0; i<name.length; i++) {
      velvet.put("child", i, name[i]);
      indexLink.connect(i);
    }
  }

  protected abstract IVelvet createVelvet();
  
  @Test
  public void testGreaterOrEq() {
    check(velvet, indexLink, IndexQueryFactory.greaterOrEq(5L),     "a5", "b5", "c5", "a7", "b7", "a9", "b9");
  }
  
  @Test
  public void testEqualsTo() {
    check(velvet, indexLink, IndexQueryFactory.equalsTo(7L),        "a7", "b7");
  }
  
  @Test
  public void testEqualsToNonExisting() {
    check(velvet, indexLink, IndexQueryFactory.equalsTo(8L)          );
  }
  
  @Test
  public void testGreater() {
    check(velvet, indexLink, IndexQueryFactory.greater(5L),         "a7", "b7", "a9", "b9");
  }
  
  @Test
  public void testFirst() {
    check(velvet, indexLink, IndexQueryFactory.first(),            "0");
  }
  
  @Test
  public void testLast() {
    check(velvet, indexLink, IndexQueryFactory.last(),             "b9");
  }
  
  @Test
  public void testPrev() {
    check(velvet, indexLink, IndexQueryFactory.prevKey(8),         "a9"); // b9 -> a9
  }
  
  @Test
  public void testPrevNil() {
    check(velvet, indexLink, IndexQueryFactory.prevKey(0)          ); // 0 -> nil
  }
  
  @Test
  public void testLess() {
    check(velvet, indexLink, IndexQueryFactory.less(5L),            "0", "1");
  }
  
  @Test
  public void testLessOrEq() {
    check(velvet, indexLink, IndexQueryFactory.lessOrEq(5L),        "0", "1", "a5", "b5", "c5");
  }
  
  
  @Test
  public void testNext1() {
    check(velvet, indexLink, IndexQueryFactory.nextKey(3),         "c5"); // b5 -> c5
  }
  
  
  @Test
  public void testNext2() {
    check(velvet, indexLink, IndexQueryFactory.nextKey(4),            "a7"); // c5 -> a7
  }
  
  @Test
  public void testNextEnd() {
    check(velvet, indexLink, IndexQueryFactory.nextKey(8)                 ); // b9 -> nil
  }
  
  private void check(IVelvet raw, IKeyIndexLink<Integer> indexLink, IIndexQuery<Integer> query, String...v) {
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
