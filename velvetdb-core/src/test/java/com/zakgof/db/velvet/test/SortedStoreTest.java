package com.zakgof.db.velvet.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.ISortableEntityDef;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.Queries;

public class SortedStoreTest extends AVelvetTxnTest {

  private ISortableEntityDef<Integer, TestEnt2> ENTITY2 = Entities.sorted(TestEnt2.class);
  private ISortableEntityDef<Integer, TestEnt3> ENTITY_EMPTY = Entities.sorted(Integer.class, TestEnt3.class, "realpojo", TestEnt3::getKey);

  @Before
  public void init() {
    
    // v1 v2 v3 v5 v7    
    ENTITY2.put(velvet, new TestEnt2(7));
    ENTITY2.put(velvet, new TestEnt2(5)); 
    ENTITY2.put(velvet, new TestEnt2(2));
    ENTITY2.put(velvet, new TestEnt2(3));
    ENTITY2.put(velvet, new TestEnt2(1));
    
  }

  @Test
  public void testGetAll() {
    check(Queries.<Integer, Integer>builder().build(),  1, 2, 3, 5, 7);
    check(Queries.<Integer, Integer>builder().descending().build(), 7, 5, 3, 2, 1);
  }
  
  @Test
  public void testStringOrder() {
    final ISortableEntityDef<String, TestEnt> ENTITY = Entities.sorted(TestEnt.class);
    ENTITY.put(velvet, new TestEnt("Aaa",    7.1f));
    ENTITY.put(velvet, new TestEnt("ab",     9.1f));    
    ENTITY.put(velvet, new TestEnt("",       0.1f));
    ENTITY.put(velvet, new TestEnt("Az",     6.1f));     
    ENTITY.put(velvet, new TestEnt("b",      1.1f));    
    ENTITY.put(velvet, new TestEnt("Aa",     5.1f));
    ENTITY.put(velvet, new TestEnt("a",      0.1f));
    ENTITY.put(velvet, new TestEnt("aaaa",   8.1f));
    ENTITY.put(velvet, new TestEnt("Aaa",    7.1f));
    ENTITY.put(velvet, new TestEnt("ab",     9.1f));    
    List<String> list = ENTITY.get(velvet).stream().map(TestEnt::getKey).collect(Collectors.toList());
    Assert.assertEquals(Arrays.asList("", "Aa", "Aaa", "Az", "a", "aaaa", "ab", "b"), list);
  }
  
  @Test
  public void testGreater() {
    check(Queries.greater(0),                                1, 2, 3, 5, 7);
    check(Queries.greater(1),                                2, 3, 5, 7);
    check(Queries.greater(4),                                5, 7);
    check(Queries.greater(5),                                7);
    check(Queries.greater(7)                                 );
    check(Queries.greater(8)                                 );    
  }
  
  @Test
  public void testGreaterOrEquals() {
    check(Queries.greaterOrEq(0),                            1, 2, 3, 5, 7);
    check(Queries.greaterOrEq(1),                            1, 2, 3, 5, 7);
    check(Queries.greaterOrEq(4),                            5, 7);
    check(Queries.greaterOrEq(5),                            5, 7);
    check(Queries.greaterOrEq(7),                            7);
    check(Queries.greaterOrEq(8)                             );
  }
  
  @Test
  public void testLess() {
    check(Queries.less(0)                                    );
    check(Queries.less(1)                                    );
    check(Queries.less(4),                                   1, 2, 3);
    check(Queries.less(5),                                   1, 2, 3);
    check(Queries.less(7),                                   1, 2, 3, 5);
    check(Queries.less(8),                                   1, 2, 3, 5, 7);
  }
  
  @Test
  public void testLessOrEquals() {
    check(Queries.lessOrEq(0)                                );
    check(Queries.lessOrEq(1),                               1);
    check(Queries.lessOrEq(4),                               1, 2, 3);
    check(Queries.lessOrEq(5),                               1, 2, 3, 5);
    check(Queries.lessOrEq(7),                               1, 2, 3, 5, 7);
    check(Queries.lessOrEq(8),                               1, 2, 3, 5, 7);
  }
  
  @Test
  public void testFirstLast() {
    check(Queries.first(),                                   1);
    check(Queries.last(),                                    7);    
  }
  
  @Test
  public void testNext() {
    check(Queries.next(0),                                   1);
    check(Queries.next(1),                                   2);
    check(Queries.next(4),                                   5);
    check(Queries.next(5),                                   7);
    check(Queries.next(7)                                     );
    check(Queries.next(8)                                     );    
  }
  
  @Test
  public void testPrev() {
    check(Queries.prev(0)                                     );
    check(Queries.prev(1)                                     );
    check(Queries.prev(4),                                   3);
    check(Queries.prev(5),                                   3);
    check(Queries.prev(7),                                   5);
    check(Queries.prev(8),                                   7);    
  }
  
  @Test
  public void testRange() {
    check(Queries.range(0, true,  8, true),                   1, 2, 3, 5, 7);
    check(Queries.range(0, false, 8, false),                  1, 2, 3, 5, 7);
    check(Queries.range(0, true,  8, true),                   1, 2, 3, 5, 7);
    check(Queries.range(0, true,  8, true),                   1, 2, 3, 5, 7);
    check(Queries.range(0, true,  8, true),                   1, 2, 3, 5, 7);
    check(Queries.range(0, true,  8, true),                   1, 2, 3, 5, 7);
        
  }
  
  void check(IRangeQuery<Integer, Integer> query, Integer... ref) {
    List<TestEnt2> result = ENTITY2.get(velvet, query);
    Assert.assertEquals(Arrays.stream(ref).collect(Collectors.toList()), result.stream().map(TestEnt2::getKey).collect(Collectors.toList()));
    List<TestEnt3> resultE = ENTITY_EMPTY.get(velvet, query);
    Assert.assertEquals(Collections.emptyList(), resultE);
  }
  
}
