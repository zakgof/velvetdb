package com.zakgof.db.velvet.test;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IEntityDef;

public class PutGetTest extends AVelvetTxnTest {

    private static final int COUNT = 6;
    private static final int HALFCOUNT = COUNT / 2;
    private IEntityDef<String, TestEnt> ENTITY = Entities.create(TestEnt.class);

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
        List<TestEnt> allValues = ENTITY.batchGetAllList(velvet);
        Assert.assertEquals(COUNT, allValues.size());
        // cleanup
        for (int d = 0; d < COUNT; d++) {
            ENTITY.deleteKey(velvet, "key" + d);
        }
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
        List<TestEnt> allValues = ENTITY.batchGetAllList(velvet);
        Assert.assertEquals(COUNT, allValues.size());
     // cleanup
        for (int d = 0; d < COUNT; d++) {
            ENTITY.deleteKey(velvet, "key" + d);
        }
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
        List<TestEnt> allValues = ENTITY.batchGetAllList(velvet);
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
        // cleanup
        for (int d = HALFCOUNT; d < COUNT; d++) {
            ENTITY.deleteKey(velvet, "key" + d);
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
        // cleanup
        for (int d = HALFCOUNT; d < COUNT; d++) {
            ENTITY.deleteKey(velvet, "key" + d);
        }
    }

    @Test
    public void testEntityAttrs() {
        Assert.assertEquals("testent", ENTITY.getKind());
        Assert.assertEquals(String.class, ENTITY.getKeyClass());
        Assert.assertEquals(TestEnt.class, ENTITY.getValueClass());
        Assert.assertEquals("k", ENTITY.keyOf(new TestEnt("k", 1.3f)));
    }

    @Test
    public void testContains() {
        for (int d = 0; d < COUNT; d++) {
            TestEnt e = new TestEnt("key" + d, d * 0.001f);
            ENTITY.put(velvet, e);
        }
        for (int d = 0; d < COUNT; d++) {
            boolean testYes = ENTITY.containsKey(velvet, "key" + d);
            Assert.assertTrue(testYes);
            boolean testNo = ENTITY.containsKey(velvet, "nokey" + d);
            Assert.assertFalse(testNo);
        }
        Assert.assertEquals(COUNT, ENTITY.size(velvet));
        // cleanup
        for (int d = 0; d < COUNT; d++) {
            ENTITY.deleteKey(velvet, "key" + d);
        }
    }

}
