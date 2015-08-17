package com.zakgof.db.velvet.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.entity.impl.Entities;
import com.zakgof.db.velvet.api.link.Links;
import com.zakgof.db.velvet.api.link.PriIndexMultiLinkDef;
import com.zakgof.db.velvet.api.query.IIndexQuery;
import com.zakgof.db.velvet.api.query.IndexQueryFactory;

public class PrimaryIndexTest {

  private IVelvet velvet; 
  
  private IEntityDef<String, TestEnt> ENTITY = Entities.create(TestEnt.class);
  private IEntityDef<Integer, TestEnt2> ENTITY2 = Entities.create(TestEnt2.class);
  private PriIndexMultiLinkDef<String, TestEnt, Integer, TestEnt2> MULTI = Links.pri(ENTITY, ENTITY2);

  private TestEnt root;

  @After
  public void rollback() {
    velvet.rollback();
  }

  public PrimaryIndexTest() {
    
    velvet = VelvetTestSuite.velvetProvider.get();
    
    Integer[] keys = new Integer[] {7, 2, 9, 1, 4, 8, 3, 6};
    root = new TestEnt("root", 1.0f);
    ENTITY.put(velvet, root);
    
    for (Integer key : keys) {
      TestEnt2 e2 = new TestEnt2(key);
      ENTITY2.put(velvet, e2);
      MULTI.connect(velvet, root, e2);
    }
    
  }

  @Test
  public void testGreaterOrEq() {
    check(IndexQueryFactory.greaterOrEq(-1),     1, 2, 3, 4, 6, 7, 8, 9);
    check(IndexQueryFactory.greaterOrEq(1),      1, 2, 3, 4, 6, 7, 8, 9);
    check(IndexQueryFactory.greaterOrEq(5),      6, 7, 8, 9);
    check(IndexQueryFactory.greaterOrEq(6),      6, 7, 8, 9);
    check(IndexQueryFactory.greaterOrEq(9),      9);
    check(IndexQueryFactory.greaterOrEq(10)      );
  }
  
  
  @Test
  public void testEqualsTo() {
    check(IndexQueryFactory.equalsTo(4),         4);
    check(IndexQueryFactory.equalsTo(1),         1);
    check(IndexQueryFactory.equalsTo(9),         9);
    check(IndexQueryFactory.equalsTo(-1));
    check(IndexQueryFactory.equalsTo(5));
    check(IndexQueryFactory.equalsTo(10));
  }
 
  @Test
  public void testGreater() {
    check(IndexQueryFactory.greater(-1),     1, 2, 3, 4, 6, 7, 8, 9);
    check(IndexQueryFactory.greater(1),      2, 3, 4, 6, 7, 8, 9);
    check(IndexQueryFactory.greater(5),      6, 7, 8, 9);
    check(IndexQueryFactory.greater(6),      7, 8, 9);
    check(IndexQueryFactory.greater(9));
    check(IndexQueryFactory.greater(10));
  }
  
  @Test
  public void testFirstLast() {
    check(IndexQueryFactory.first(),              1);
    check(IndexQueryFactory.last(),               9);
  }
   
  @Test
  public void testNext() {
    check(IndexQueryFactory.next(-1),        1);
    check(IndexQueryFactory.next(1),         2);
    check(IndexQueryFactory.next(4),         6);
    check(IndexQueryFactory.next(9));
  }
  
  /*
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

  */
  
  private void check(IIndexQuery query, int...v) {
    int[] keys = MULTI.indexed(query).multi(velvet, root).stream().mapToInt(TestEnt2::getKey).toArray();
    Assert.assertArrayEquals(v, keys);
  }

}
