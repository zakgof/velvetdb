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
  public void testGreaterOrEqDesc() {
    check(IndexQueryFactory.builder().descending().greaterOrEqK(-1).build(),     9, 8, 7, 6, 4, 3, 2, 1);
    check(IndexQueryFactory.builder().descending().greaterOrEqK(1).build(),      9, 8, 7, 6, 4, 3, 2, 1);
    check(IndexQueryFactory.builder().descending().greaterOrEqK(5).build(),      9, 8, 7, 6);
    check(IndexQueryFactory.builder().descending().greaterOrEqK(6).build(),      9, 8, 7, 6);
    check(IndexQueryFactory.builder().descending().greaterOrEqK(9).build(),      9);
    check(IndexQueryFactory.builder().descending().greaterOrEqK(10).build()      );
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
  
  @Test
  public void testLess() {
    check(IndexQueryFactory.less(-1));
    check(IndexQueryFactory.less(1));
    check(IndexQueryFactory.less(2),         1);
    check(IndexQueryFactory.less(5),         1, 2, 3, 4);
    check(IndexQueryFactory.less(6),         1, 2, 3, 4);
    check(IndexQueryFactory.less(9),         1, 2, 3, 4, 6, 7, 8);    
    check(IndexQueryFactory.less(10),        1, 2, 3, 4, 6, 7, 8, 9);
  }
  
  @Test
  public void testLessOrEq() {
    check(IndexQueryFactory.lessOrEq(-1));
    check(IndexQueryFactory.lessOrEq(1),         1);
    check(IndexQueryFactory.lessOrEq(2),         1, 2);
    check(IndexQueryFactory.lessOrEq(5),         1, 2, 3, 4);
    check(IndexQueryFactory.lessOrEq(6),         1, 2, 3, 4, 6);
    check(IndexQueryFactory.lessOrEq(9),         1, 2, 3, 4, 6, 7, 8, 9);    
    check(IndexQueryFactory.lessOrEq(10),        1, 2, 3, 4, 6, 7, 8, 9);
  }
  
  @Test
  public void testRange() {
    check(IndexQueryFactory.range(-1, true, 10, true),      1, 2, 3, 4, 6, 7, 8, 9);
    check(IndexQueryFactory.range(-1, false, 10, false),    1, 2, 3, 4, 6, 7, 8, 9);    
    check(IndexQueryFactory.range(0, true, 9, false),     1, 2, 3, 4, 6, 7, 8);
    check(IndexQueryFactory.range(1, false, 9, true),     2, 3, 4, 6, 7, 8, 9);
    check(IndexQueryFactory.range(2, true, 7, false),     2, 3, 4, 6);
    check(IndexQueryFactory.range(2, true, 6, true),      2, 3, 4, 6);
    check(IndexQueryFactory.range(5, true, 6, false));
    check(IndexQueryFactory.range(6, true, 6, true),      6);
    check(IndexQueryFactory.range(6, true, 6, false));
    check(IndexQueryFactory.range(6, false, 6, true));
    check(IndexQueryFactory.range(5, true, 2, true));    
  }
  
  @Test
  public void testRangeDesc() {
    check(IndexQueryFactory.builder().descending().greaterOrEqK(2).lessK(8).build(),   7, 6, 4, 3, 2);     
    check(IndexQueryFactory.builder().descending().greaterOrEqK(8).lessK(8).build());
    check(IndexQueryFactory.builder().descending().greaterK(0).lessK(3).build(),   2, 1);
    check(IndexQueryFactory.builder().lessOrEqK(7).descending().greaterK(5).build(),  7, 6);
    check(IndexQueryFactory.builder().lessK(10).descending().greaterOrEqK(5).build(),  9, 8, 7, 6);
  }
  
  @Test
  public void testLimitOffset() {
    check(IndexQueryFactory.builder().greaterOrEqK(2).lessK(8).limit(2).build(),        2, 3);
    check(IndexQueryFactory.builder().greaterOrEqK(2).lessK(8).limit(10).build(),       2, 3, 4, 6, 7);
    check(IndexQueryFactory.builder().greaterOrEqK(2).lessK(8).limit(10).offset(2).build(),  4, 6, 7);
    check(IndexQueryFactory.builder().greaterOrEqK(2).lessK(8).limit(2).offset(2).build(),  4, 6);
    check(IndexQueryFactory.builder().greaterOrEqK(2).lessK(8).limit(1).offset(10).build());    
    check(IndexQueryFactory.builder().greaterOrEqK(2).lessK(8).limit(1).descending().offset(1).build(),     6);
    check(IndexQueryFactory.builder().greaterOrEqK(2).lessK(8).limit(10).descending().offset(1).build(),     6, 5, 4, 3, 2, 1);
    check(IndexQueryFactory.builder().greaterOrEqK(2).lessK(8).limit(1).descending().offset(5).build(),      2);
    check(IndexQueryFactory.builder().limit(4).build(),                     2, 3, 4, 6);
    check(IndexQueryFactory.builder().limit(4).descending().build(),        9, 8, 7, 6);
    check(IndexQueryFactory.builder().limit(4).offset(2).build(),                    4, 6, 7, 8);
    check(IndexQueryFactory.builder().limit(4).descending().offset(5).build(),       3, 2, 1);
  }
  
  private void check(IIndexQuery query, int...v) {
    int[] keys = MULTI.indexed(query).multi(velvet, root).stream().mapToInt(TestEnt2::getKey).toArray();
    Assert.assertArrayEquals(v, keys);
  }

}
