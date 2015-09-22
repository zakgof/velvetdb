package com.zakgof.db.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.impl.link.PriIndexMultiLinkDef;
import com.zakgof.db.velvet.link.Links;
import com.zakgof.db.velvet.query.IIndexQuery;
import com.zakgof.db.velvet.query.IndexQueryFactory;

public class PrimaryIndexTest extends AVelvetTxnTest {
  
  private IEntityDef<String, TestEnt> ENTITY = Entities.anno(TestEnt.class);
  private IEntityDef<Integer, TestEnt2> ENTITY2 = Entities.anno(TestEnt2.class);
  private PriIndexMultiLinkDef<String, TestEnt, Integer, TestEnt2> MULTI = Links.pri(ENTITY, ENTITY2);

  private TestEnt root;

  @Before
  public void init() {
    
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
    check(IndexQueryFactory.<Integer>builder().descending().greaterOrEq(-1).build(),     9, 8, 7, 6, 4, 3, 2, 1);
    check(IndexQueryFactory.<Integer>builder().descending().greaterOrEq(1).build(),      9, 8, 7, 6, 4, 3, 2, 1);
    check(IndexQueryFactory.<Integer>builder().descending().greaterOrEq(5).build(),      9, 8, 7, 6);
    check(IndexQueryFactory.<Integer>builder().descending().greaterOrEq(6).build(),      9, 8, 7, 6);
    check(IndexQueryFactory.<Integer>builder().descending().greaterOrEq(9).build(),      9);
    check(IndexQueryFactory.<Integer>builder().descending().greaterOrEq(10).build()      );
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
  
  private void check(IIndexQuery<Integer> query, int...v) {
    int[] keys = MULTI.indexed(query).multi(velvet, root).stream().mapToInt(TestEnt2::getKey).toArray();
    Assert.assertArrayEquals(v, keys);
  }

}
