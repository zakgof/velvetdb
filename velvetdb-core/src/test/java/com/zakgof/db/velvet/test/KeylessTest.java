package com.zakgof.db.velvet.test;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.annimon.stream.Collectors;
import com.annimon.stream.Stream;
import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IKeylessEntityDef;
import com.zakgof.db.velvet.query.Queries;

public class KeylessTest extends AVelvetTxnTest {

  private IKeylessEntityDef<KeylessEnt> ENTITY = Entities.keyless(KeylessEnt.class);

  @Before
  public void init() {
    // v1 v2 v3 v5 v7
    ENTITY.put(velvet, new KeylessEnt(7, "seven"));
    ENTITY.put(velvet, new KeylessEnt(5, "five"));
    ENTITY.put(velvet, new KeylessEnt(2, "two"));
    ENTITY.put(velvet, new KeylessEnt(3, "three"));
    ENTITY.put(velvet, new KeylessEnt(1, "one"));
  }

  @Test
  public void testGetAll() {
    List<Integer> all = Stream.of(ENTITY.get(velvet)).map(KeylessEnt::getNum).collect(Collectors.toList());
    Assert.assertEquals(Arrays.asList(7, 5, 2, 3, 1), all);
  }

  @Test
  public void testTraverse() {
      KeylessEnt first = ENTITY.get(velvet, Queries.first());
      Assert.assertEquals("seven", first.getStr());

      KeylessEnt e2 = ENTITY.get(velvet, Queries.next(ENTITY.keyOf(first)));
      Assert.assertEquals("five", e2.getStr());

      KeylessEnt e3 = ENTITY.get(velvet, Queries.next(ENTITY.keyOf(e2))); // TODO: helper for this
      Assert.assertEquals("two", e3.getStr());

      KeylessEnt last = ENTITY.get(velvet, Queries.last());
      Assert.assertEquals("one", last.getStr());
  }

  @Test
  public void testGetByKey() {
      KeylessEnt first = ENTITY.get(velvet, Queries.first());
      Long k = ENTITY.keyOf(first);
      Assert.assertNotNull(k);
      KeylessEnt reget = ENTITY.get(velvet, k);
      Assert.assertEquals(first, reget);
  }

}
