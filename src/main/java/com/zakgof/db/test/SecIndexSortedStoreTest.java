package com.zakgof.db.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.ISortableEntityDef;
import com.zakgof.db.velvet.query.IIndexQuery;
import com.zakgof.db.velvet.query.Queries;

public class SecIndexSortedStoreTest extends AVelvetTxnTest {

  private ISortableEntityDef<Integer, TestEnt2> ENTITY2 = Entities.sortedAnno(TestEnt2.class);

  public void init() {
    
    // v1 1a 1b v2 v3 3a v5 v7 7a 7b    
    ENTITY2.put(velvet, new TestEnt2(1, "1a"));
    ENTITY2.put(velvet, new TestEnt2(7));    
    ENTITY2.put(velvet, new TestEnt2(7, "7b"));
    ENTITY2.put(velvet, new TestEnt2(3, "3a"));   
    ENTITY2.put(velvet, new TestEnt2(5)); 
    ENTITY2.put(velvet, new TestEnt2(7, "7a"));
    ENTITY2.put(velvet, new TestEnt2(1, "1b"));
    ENTITY2.put(velvet, new TestEnt2(2));
    ENTITY2.put(velvet, new TestEnt2(3));
    ENTITY2.put(velvet, new TestEnt2(5));
    ENTITY2.put(velvet, new TestEnt2(1));
    
  }

  @Test
  public void testGetAll() {
    check(Queries.<Integer>builder().build(),  r("v1", "1a", "1b"), "v2", r("v3", "3a"), "v5", r("v7", "7a", "7b"));
    check(Queries.<Integer>builder().descending().build(), r("v7", "7a", "7b"), "v5",  r("v3", "3a"), "2", r("v1", "1a", "1b"));
  }
  
  private Object r(String... s) {
    return s;
  }

  void check(IIndexQuery<Integer> query, Object... ref) {
    List<TestEnt2> result = ENTITY2.get(velvet, query);
    int i = 0;
    for (Object r : ref) {
      if (r instanceof String) {
        Assert.assertEquals("Mismatch at position " + i + ":", r, result.get(i).getVal());
        i++;
      } else if (r instanceof String[]) {
        String[] refarr = (String[])r;
        Set<String> refSet = new HashSet<>(Arrays.asList(refarr));
        Set<String> actSet = result.subList(i, i + refarr.length).stream().map(TestEnt2::getVal).collect(Collectors.toSet());
        Assert.assertEquals("Mismatch at position " + i + ":", refSet, actSet);
        i += refarr.length;
      }
    }
    Assert.assertEquals(i, result.size());
  }
  
  
}
