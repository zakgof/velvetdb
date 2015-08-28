package com.zakgof.db.velvet.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.entity.impl.Entities;
import com.zakgof.db.velvet.api.link.Links;
import com.zakgof.db.velvet.api.link.SecIndexMultiLinkDef;
import com.zakgof.db.velvet.api.query.IIndexQuery;
import com.zakgof.db.velvet.api.query.IndexQueryFactory;

public class SecondaryIndexTest {

  private IVelvet velvet; 
  
  private IEntityDef<String, TestEnt> ENTITY = Entities.create(TestEnt.class);
  private IEntityDef<Integer, TestEnt3> ENTITY3 = Entities.create(Integer.class, TestEnt3.class, "realpojo", TestEnt3::getKey);
  private SecIndexMultiLinkDef<String, TestEnt, Integer, TestEnt3, Long> MULTI = Links.sec(ENTITY, ENTITY3, Long.class, TestEnt3::getWeight);

  private TestEnt root;

  @After
  public void rollback() {
    velvet.rollback();
  }

  public SecondaryIndexTest() {
    
    velvet = VelvetTestSuite.velvetProvider.get();
    
    TestEnt3[] vals = new TestEnt3[] {
       new TestEnt3(54, 1L, "one-A"),
       new TestEnt3(44, 1L, "one-B"),
       new TestEnt3(33, 2L, "two"),
       new TestEnt3(21, 3L, "three"),
       new TestEnt3(34, 4L, "four-A"),
       new TestEnt3(47, 4L, "four-B"),
       new TestEnt3(60, 4L, "four-C"),
       new TestEnt3(99, 6L, "six-A"),
       new TestEnt3(31, 6L, "six-B"),
    };
    root = new TestEnt("root", 1.0f);
    ENTITY.put(velvet, root);
    
    for (TestEnt3 val : vals) {
      ENTITY3.put(velvet, val);
      MULTI.connect(velvet, root, val);
    }
    
  }

  @Test
  public void testGreaterOrEq() {
    check(IndexQueryFactory.greaterOrEq(-1L),     "one-A", "one-B", "two", "three", "four-A", "four-B", "four-C", "six-A",  "six-B");
    check(IndexQueryFactory.greaterOrEq(1L),      "one-A", "one-B", "two", "three", "four-A", "four-B", "four-C", "six-A",  "six-B");
    check(IndexQueryFactory.greaterOrEq(3L),      "three", "four-A", "four-B", "four-C", "six-A",  "six-B");
    check(IndexQueryFactory.greaterOrEq(4L),      "four-A", "four-B", "four-C", "six-A",  "six-B");
    check(IndexQueryFactory.greaterOrEq(5L),      "six-A",  "six-B");
    check(IndexQueryFactory.greaterOrEq(6L),      "six-A",  "six-B");
    check(IndexQueryFactory.greaterOrEq(7L)        );
  }
  
  @Test
  public void testGreaterOrEqDesc() {
    check(IndexQueryFactory.<Long>builder().descending().greaterOrEq(-1L).build(),     "six-B", "six-A", "four-C", "four-B", "four-A", "three", "two", "one-B", "one-A");  
    check(IndexQueryFactory.<Long>builder().descending().greaterOrEq(1L).build(),      "six-B", "six-A", "four-C", "four-B", "four-A", "three", "two", "one-B", "one-A");
    check(IndexQueryFactory.<Long>builder().descending().greaterOrEq(3L).build(),      "six-B", "six-A", "four-C", "four-B", "four-A", "three");
    check(IndexQueryFactory.<Long>builder().descending().greaterOrEq(4L).build(),      "six-B", "six-A", "four-C", "four-B", "four-A");
    check(IndexQueryFactory.<Long>builder().descending().greaterOrEq(5L).build(),      "six-B", "six-A");
    check(IndexQueryFactory.<Long>builder().descending().greaterOrEq(6L).build(),      "six-B", "six-A");
    check(IndexQueryFactory.<Long>builder().descending().greaterOrEq(7L).build()      );
  }
  
  
  @Test
  public void testEqualsTo() {
    check(IndexQueryFactory.equalsTo(-1L)      );
    check(IndexQueryFactory.equalsTo(1L),      "one-A", "one-B");
    check(IndexQueryFactory.equalsTo(3L),      "three");
    check(IndexQueryFactory.equalsTo(4L),      "four-A", "four-B", "four-C");
    check(IndexQueryFactory.equalsTo(5L)       );
    check(IndexQueryFactory.equalsTo(6L),      "six-A",  "six-B");
    check(IndexQueryFactory.equalsTo(7L)       );
  }
 
  @Test
  public void testGreater() {
    check(IndexQueryFactory.greater(-1L),     "one-A", "one-B", "two", "three", "four-A", "four-B", "four-C", "six-A", "six-B");
    check(IndexQueryFactory.greater(1L),      "two", "three", "four-A", "four-B", "four-C", "six-A",  "six-B");
    check(IndexQueryFactory.greater(3L),      "four-A", "four-B", "four-C", "six-A",  "six-B");
    check(IndexQueryFactory.greater(4L),      "six-A",  "six-B");
    check(IndexQueryFactory.greater(5L),      "six-A",  "six-B");
    check(IndexQueryFactory.greater(6L)        );
    check(IndexQueryFactory.greater(7L)        );
  }
  
  @Test
  public void testFirstLast() {
    check(IndexQueryFactory.first(),           "one-A");
    check(IndexQueryFactory.last(),            "six-B");
  }
  
  /*
   
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
    check(IndexQueryFactory.<Integer>builder().descending().greaterOrEq(2).less(8).build(),   7, 6, 4, 3, 2);     
    check(IndexQueryFactory.<Integer>builder().descending().greaterOrEq(8).less(8).build());
    check(IndexQueryFactory.<Integer>builder().descending().greater(0).less(3).build(),   2, 1);
    check(IndexQueryFactory.<Integer>builder().lessOrEq(7).descending().greater(5).build(),  7, 6);
    check(IndexQueryFactory.<Integer>builder().less(10).descending().greaterOrEq(5).build(),  9, 8, 7, 6);
  }
  
  @Test
  public void testLimitOffset() {
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(2).build(),        2, 3);
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(10).build(),       2, 3, 4, 6, 7);
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(10).offset(2).build(),  4, 6, 7);
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(2).offset(2).build(),  4, 6);
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(1).offset(10).build());    
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(1).descending().offset(1).build(),     6);
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(10).descending().offset(1).build(),     6, 4, 3, 2);
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(1).descending().offset(4).build(),      2);
    check(IndexQueryFactory.<Integer>builder().limit(4).build(),                     1, 2, 3, 4);
    check(IndexQueryFactory.<Integer>builder().limit(4).descending().build(),        9, 8, 7, 6);
    check(IndexQueryFactory.<Integer>builder().limit(4).offset(2).build(),                    3, 4, 6, 7);
    check(IndexQueryFactory.<Integer>builder().limit(4).descending().offset(5).build(),       3, 2, 1);
  }
  */
  
  private void check(IIndexQuery<Long> query, String...v) {
    String[] vals = MULTI.indexed(query).multi(velvet, root).stream().map(TestEnt3::getStr).toArray(n -> new String[n]);
    Assert.assertArrayEquals(v, vals);
  }

}
