package com.zakgof.db.velvet.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.ISortableEntityDef;
import com.zakgof.db.velvet.api.entity.impl.Entities;
import com.zakgof.db.velvet.api.query.IIndexQuery;
import com.zakgof.db.velvet.api.query.IndexQueryFactory;

public class SortedStoreTest {

  private IVelvet velvet;

  private ISortableEntityDef<Integer, TestEnt2> ENTITY2 = Entities.sortedAnno(TestEnt2.class);
  private ISortableEntityDef<Integer, TestEnt3> ENTITY_EMPTY = Entities.sorted(Integer.class, TestEnt3.class, "realpojo", TestEnt3::getKey);

  public SortedStoreTest() {
    velvet = VelvetTestSuite.velvetProvider.get();
    
    // v1 v2 v3 v5 v7    
    ENTITY2.put(velvet, new TestEnt2(7));    
    ENTITY2.put(velvet, new TestEnt2(5)); 
    ENTITY2.put(velvet, new TestEnt2(2));
    ENTITY2.put(velvet, new TestEnt2(3));
    ENTITY2.put(velvet, new TestEnt2(1));
    
  }

  @After
  public void rollback() {
    velvet.rollback();
  }

  @Test
  public void testGetAll() {
    check(IndexQueryFactory.<Integer>builder().build(),  1, 2, 3, 5, 7);
    check(IndexQueryFactory.<Integer>builder().descending().build(), 7, 5, 3, 2, 1);
  }
  
  @Test
  public void testGreater() {
    check(IndexQueryFactory.greater(0),                                1, 2, 3, 5, 7);
    check(IndexQueryFactory.greater(1),                                2, 3, 5, 7);
    check(IndexQueryFactory.greater(4),                                5, 7);
    check(IndexQueryFactory.greater(5),                                7);
    check(IndexQueryFactory.greater(7)                                 );
    check(IndexQueryFactory.greater(8)                                 );    
  }
  
  @Test
  public void testGreaterOrEquals() {
    check(IndexQueryFactory.greaterOrEq(0),                            1, 2, 3, 5, 7);
    check(IndexQueryFactory.greaterOrEq(1),                            1, 2, 3, 5, 7);
    check(IndexQueryFactory.greaterOrEq(4),                            5, 7);
    check(IndexQueryFactory.greaterOrEq(5),                            5, 7);
    check(IndexQueryFactory.greaterOrEq(7),                            7);
    check(IndexQueryFactory.greaterOrEq(8)                             );
  }
  
  @Test
  public void testLess() {
    check(IndexQueryFactory.less(0)                                    );
    check(IndexQueryFactory.less(1)                                    );
    check(IndexQueryFactory.less(4),                                   1, 2, 3);
    check(IndexQueryFactory.less(5),                                   1, 2, 3);
    check(IndexQueryFactory.less(7),                                   1, 2, 3, 5);
    check(IndexQueryFactory.less(8),                                   1, 2, 3, 5, 7);
  }
  
  @Test
  public void testLessOrEquals() {
    check(IndexQueryFactory.lessOrEq(0)                                );
    check(IndexQueryFactory.lessOrEq(1),                               1);
    check(IndexQueryFactory.lessOrEq(4),                               1, 2, 3);
    check(IndexQueryFactory.lessOrEq(5),                               1, 2, 3, 5);
    check(IndexQueryFactory.lessOrEq(7),                               1, 2, 3, 5, 7);
    check(IndexQueryFactory.lessOrEq(8),                               1, 2, 3, 5, 7);
  }
  
  @Test
  public void testFirstLast() {
    check(IndexQueryFactory.first(),                                   1);
    check(IndexQueryFactory.last(),                                    7);    
  }
  
  @Test
  public void testNext() {
    check(IndexQueryFactory.next(0),                                   1);
    check(IndexQueryFactory.next(1),                                   2);
    check(IndexQueryFactory.next(4),                                   5);
    check(IndexQueryFactory.next(5),                                   7);
    check(IndexQueryFactory.next(7)                                     );
    check(IndexQueryFactory.next(8)                                     );    
  }
  
  @Test
  public void testPrev() {
    check(IndexQueryFactory.prev(0)                                     );
    check(IndexQueryFactory.prev(1)                                     );
    check(IndexQueryFactory.prev(4),                                   3);
    check(IndexQueryFactory.prev(5),                                   3);
    check(IndexQueryFactory.prev(7),                                   5);
    check(IndexQueryFactory.prev(8),                                   7);    
  }
  
  @Test
  public void testRange() {
    check(IndexQueryFactory.range(0, true,  8, true),                   1, 2, 3, 5, 7);
    check(IndexQueryFactory.range(0, false, 8, false),                  1, 2, 3, 5, 7);
    check(IndexQueryFactory.range(0, true,  8, true),                   1, 2, 3, 5, 7);
    check(IndexQueryFactory.range(0, true,  8, true),                   1, 2, 3, 5, 7);
    check(IndexQueryFactory.range(0, true,  8, true),                   1, 2, 3, 5, 7);
    check(IndexQueryFactory.range(0, true,  8, true),                   1, 2, 3, 5, 7);
        
  }
  
  void check(IIndexQuery<Integer> query, Integer... ref) {
    List<TestEnt2> result = ENTITY2.get(velvet, query);
    Assert.assertEquals(Arrays.stream(ref).collect(Collectors.toList()), result.stream().map(TestEnt2::getKey).collect(Collectors.toList()));
    List<TestEnt3> resultE = ENTITY_EMPTY.get(velvet, query);
    Assert.assertEquals(Collections.emptyList(), resultE);
  }
  
}
