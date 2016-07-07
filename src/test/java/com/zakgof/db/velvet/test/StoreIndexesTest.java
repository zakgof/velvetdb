package com.zakgof.db.velvet.test;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.entity.Indexes;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.Queries;

public class StoreIndexesTest extends AVelvetTxnTest {

  private IEntityDef<Integer, TestEnt3> ENTITY3 = Entities.create(TestEnt3.class,
      Indexes.create("key", TestEnt3::getKey),
      Indexes.create("str", TestEnt3::getStr),
      Indexes.create("weight", TestEnt3::getWeight)
  );

  @Before
  public void init() {
    ENTITY3.put(velvet, new TestEnt3(1,  100L, "l"));
    ENTITY3.put(velvet, new TestEnt3(2, 1000L, "i"));
    ENTITY3.put(velvet, new TestEnt3(3,  900L, "m"));
    ENTITY3.put(velvet, new TestEnt3(4,  800L, "h"));
    ENTITY3.put(velvet, new TestEnt3(5,  300L, "e"));
    ENTITY3.put(velvet, new TestEnt3(6, 1200L, "f"));
    ENTITY3.put(velvet, new TestEnt3(7,  400L, "c"));
    ENTITY3.put(velvet, new TestEnt3(8, 1100L, "k"));
    ENTITY3.put(velvet, new TestEnt3(9,  200L, "g"));
    ENTITY3.put(velvet, new TestEnt3(10, 600L, "a"));
    ENTITY3.put(velvet, new TestEnt3(11, 700L, "b"));
    ENTITY3.put(velvet, new TestEnt3(12,1300L, "d"));
    ENTITY3.put(velvet, new TestEnt3(13, 500L, "j"));
  }

  @Test
  public void testGetAll() {
    check("key", Queries.<Integer, Integer>builder().build(), "limhefckgabdj");
    check("weight", Queries.<Integer, Integer>builder().build(), "lgecjabhmikfd");
    check("str", Queries.<Integer, Integer>builder().build(), "abcdefghijklm");
  }
  
  private <K, M extends Comparable<? super M>> void check(String name, IRangeQuery<Integer, M> query, String ref) {
    List<Integer> keys = ENTITY3.<M>index(velvet, name).keys(query);
    List<TestEnt3> values = ENTITY3.get(velvet, keys);
    String result = values.stream().map(TestEnt3::getStr).collect(Collectors.joining(""));
    Assert.assertEquals(result, ref);
  }

}
