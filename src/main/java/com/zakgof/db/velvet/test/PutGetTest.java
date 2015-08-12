package com.zakgof.db.velvet.test;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.entity.impl.Entities;

public class PutGetTest {

  private static final int COUNT = 1000;
  private static final int HALFCOUNT = COUNT / 2;
  private IVelvet velvet;
  private IEntityDef<String, TestEnt> ENTITY = Entities.create(TestEnt.class);

  public PutGetTest() {
    velvet = VelvetTestSuite.velvetProvider.get();
  }

  @After
  public void rollback() {
    velvet.rollback();
  }

  @Test
  public void testSimplePutGet() {
    for (int d = 0; d < COUNT; d++) {
      TestEnt e = new TestEnt("key" + d, d * 0.001f);
      ENTITY.put(velvet, e);
    }
    for (int d = 0; d < COUNT; d++) {
      TestEnt ent = ENTITY.get(velvet, "key" + d);
      Assert.assertNotNull(ent);
      Assert.assertEquals("key" + d, ent.getKey());
      Assert.assertEquals(d * 0.001f, ent.getVal(), 1e-5);
    }
    Assert.assertNull(ENTITY.get(velvet, "key1001"));
    List<TestEnt> allValues = ENTITY.getAll(velvet);
    Assert.assertEquals(COUNT, allValues.size());
  }

  @Test
  public void testUpdate() {
    for (int d = 0; d < COUNT; d++) {
      TestEnt e = new TestEnt("key" + d, d * 0.001f);
      ENTITY.put(velvet, e);
    }
    for (int d = 0; d < COUNT; d++) {
      TestEnt e = new TestEnt("key" + d, d * 0.002f);
      ENTITY.put(velvet, e);
    }
    for (int d = 0; d < COUNT; d++) {
      TestEnt ent = ENTITY.get(velvet, "key" + d);
      Assert.assertNotNull(ent);
      Assert.assertEquals("key" + d, ent.getKey());
      Assert.assertEquals(d * 0.002f, ent.getVal(), 1e-5);
    }
    List<TestEnt> allValues = ENTITY.getAll(velvet);
    Assert.assertEquals(COUNT, allValues.size());
  }

  @Test
  public void testDeleteKey() {
    for (int d = 0; d < COUNT; d++) {
      TestEnt e = new TestEnt("key" + d, d * 0.001f);
      ENTITY.put(velvet, e);
    }
    for (int d = 0; d < HALFCOUNT; d++) {
      ENTITY.deleteKey(velvet, "key" + d);
    }
    List<TestEnt> allValues = ENTITY.getAll(velvet);
    Assert.assertEquals(HALFCOUNT, allValues.size());
    for (int d = 0; d < HALFCOUNT; d++) {
      TestEnt ent = ENTITY.get(velvet, "key" + d);
      Assert.assertNull(ent);
    }
    for (int d = HALFCOUNT; d < COUNT; d++) {
      TestEnt ent = ENTITY.get(velvet, "key" + d);
      Assert.assertNotNull(ent);
      Assert.assertEquals("key" + d, ent.getKey());
      Assert.assertEquals(d * 0.001f, ent.getVal(), 1e-5);
    }
  }

  @Test
  public void testDeleteValue() {
    for (int d = 0; d < COUNT; d++) {
      TestEnt e = new TestEnt("key" + d, d * 0.001f);
      ENTITY.put(velvet, e);
    }
    for (int d = 0; d < HALFCOUNT; d++) {
      ENTITY.deleteValue(velvet, new TestEnt("key" + d, -1.0f));
    }
    for (int d = 0; d < HALFCOUNT; d++) {
      TestEnt ent = ENTITY.get(velvet, "key" + d);
      Assert.assertNull(ent);
    }
    for (int d = HALFCOUNT; d < COUNT; d++) {
      TestEnt ent = ENTITY.get(velvet, "key" + d);
      Assert.assertNotNull(ent);
      Assert.assertEquals("key" + d, ent.getKey());
      Assert.assertEquals(d * 0.001f, ent.getVal(), 1e-5);
    }
  }

  @Test
  public void testEntityAttrs() {
    Assert.assertEquals("testent", ENTITY.getKind());
    Assert.assertEquals(String.class, ENTITY.getKeyClass());
    Assert.assertEquals(TestEnt.class, ENTITY.getValueClass());
    Assert.assertEquals("k", ENTITY.keyOf(new TestEnt("k", 1.3f)));
  }

}
