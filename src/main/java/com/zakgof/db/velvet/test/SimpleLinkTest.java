package com.zakgof.db.velvet.test;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.entity.impl.Entities;
import com.zakgof.db.velvet.api.link.IBiManyToManyLinkDef;
import com.zakgof.db.velvet.api.link.IBiMultiLinkDef;
import com.zakgof.db.velvet.api.link.IBiSingleLinkDef;
import com.zakgof.db.velvet.api.link.IMultiLinkDef;
import com.zakgof.db.velvet.api.link.ISingleLinkDef;
import com.zakgof.db.velvet.api.link.Links;

public class SimpleLinkTest {

  private static final int COUNT = 1000;
  private IVelvet velvet;

  private IEntityDef<String, TestEnt> ENTITY = Entities.create(TestEnt.class);
  private IEntityDef<Integer, TestEnt2> ENTITY2 = Entities.create(TestEnt2.class);

  private ISingleLinkDef<String, TestEnt, Integer, TestEnt2> SINGLE = Links.single(ENTITY, ENTITY2, "single");
  private IMultiLinkDef<String, TestEnt, Integer, TestEnt2> MULTI = Links.multi(ENTITY, ENTITY2, "multi");

  private IBiSingleLinkDef<String, TestEnt, Integer, TestEnt2> ONE_TO_ONE = Links.biSingle(ENTITY, ENTITY2, "bisingle", "bisingle-back");
  private IBiMultiLinkDef<String, TestEnt, Integer, TestEnt2> ONE_TO_MANY = Links.biMulti(ENTITY, ENTITY2, "bimulti", "bimulti-back");
  private IBiManyToManyLinkDef<String, TestEnt, Integer, TestEnt2> MANY_TO_MANY = Links.biManyToMany(ENTITY, ENTITY2, "many", "many-back");

  public SimpleLinkTest() {
    velvet = VelvetTestSuite.velvetProvider.get();
  }

  @After
  public void rollback() {
    velvet.rollback();
  }

  @Test
  public void testSingleLink() {
    testSingleLink(SINGLE);
    testSingleLink(ONE_TO_ONE);
  }
  
  @Test
  public void testSingleLinkReconnect() {
    testSingleLinkReconnect(SINGLE);
    testSingleLinkReconnect(ONE_TO_ONE);
  }
  
  @Test
  public void testSingleLinkDisconnect() {
    testSingleLinkDisconnect(SINGLE);
    testSingleLinkDisconnect(ONE_TO_ONE);
  }
  
  @Test
  public void testMultiLink() {
    testMultiLink(MULTI);
    testMultiLink(ONE_TO_MANY);
    testMultiLink(MANY_TO_MANY);
  }
  
  @Test
  public void testMultiLinkDisconnect() {
    testMultiLinkDisconnect(MULTI);
    testMultiLinkDisconnect(ONE_TO_MANY);
    testMultiLinkDisconnect(MANY_TO_MANY);
  }
  
  @Test
  public void testMultiLinkEmpty() {        
    testMultiLinkEmpty(MULTI);
    testMultiLinkEmpty(ONE_TO_MANY);
    testMultiLinkEmpty(MANY_TO_MANY);
  }
  
  @Test
  public void testMultiLinkOverwrite() {        
    testMultiLinkOverwrite(MULTI);
    testMultiLinkOverwrite(ONE_TO_MANY);
    testMultiLinkOverwrite(MANY_TO_MANY);
  }
  
  private void fillEntities() {
    for (int d = 0; d < COUNT; d++) {
      TestEnt e = new TestEnt("key" + d, d * 0.001f);
      ENTITY.put(velvet, e);
      TestEnt2 e2 = new TestEnt2(d);
      ENTITY2.put(velvet, e2);
    }
  }

  private void testSingleLink(ISingleLinkDef<String, TestEnt, Integer, TestEnt2> single) {
    fillEntities();

    TestEnt e = new TestEnt("key13", 0.013f);
    TestEnt2 e2 = new TestEnt2(99);

    TestEnt xe = new TestEnt("key23", 0.023f);
    TestEnt2 xe2 = new TestEnt2(77);

    TestEnt ze = new TestEnt("key29", 0.029f);

    
    single.connect(velvet, e, e2);
    single.connect(velvet, xe, xe2);

    TestEnt2 t2 = single.single(velvet, e);
    Assert.assertNotNull(t2);
    Assert.assertEquals("v99", t2.getVal());

    Integer t2key = single.singleKey(velvet, "key23");
    Assert.assertEquals(new Integer(77), t2key);

    TestEnt2 tz = single.single(velvet, ze);
    Assert.assertNull(tz);
    Integer tzkey = single.singleKey(velvet, "key56");
    Assert.assertNull(tzkey);
  }
  
  private void testSingleLinkReconnect(ISingleLinkDef<String, TestEnt, Integer, TestEnt2> single) {
    fillEntities();
    
    TestEnt parent = new TestEnt("key13", 0.013f);
    TestEnt2 child1 = new TestEnt2(25);
    TestEnt2 child2 = new TestEnt2(29);
    
    single.connect(velvet, parent, child1);
    single.connect(velvet, parent, child2);
    
    TestEnt2 child = single.single(velvet, parent);
    Assert.assertNotNull(child);
    Assert.assertEquals(child2.getKey(), child.getKey());
    
    Assert.assertTrue(single.isConnected(velvet, parent, child2));
    Assert.assertFalse(single.isConnected(velvet, parent, child1));
  }
  
  private void testSingleLinkDisconnect(ISingleLinkDef<String, TestEnt, Integer, TestEnt2> single) {
    fillEntities();
    
    TestEnt parent = new TestEnt("key13", 0.013f);
    TestEnt2 child1 = new TestEnt2(25);
        
    
    single.connect(velvet, parent, child1);
    single.disconnect(velvet, parent, child1);
    
    TestEnt2 child = single.single(velvet, parent);
    Assert.assertNull(child);
    Assert.assertFalse(single.isConnected(velvet, parent, child1));
  }

  private void testMultiLink(IMultiLinkDef<String, TestEnt, Integer, TestEnt2> multi) {
    fillEntities();
    
    TestEnt parent = new TestEnt("key30", 0.03f);
    TestEnt2 child1 = new TestEnt2(25);
    TestEnt2 child2 = new TestEnt2(77);
    TestEnt2 child3 = new TestEnt2(99);
        
    
    multi.connect(velvet, parent, child1);
    multi.connect(velvet, parent, child2);
    multi.connect(velvet, parent, child3);
    
    Assert.assertTrue(multi.isConnected(velvet, parent, child1));
    Assert.assertTrue(multi.isConnected(velvet, parent, child2));
    Assert.assertTrue(multi.isConnected(velvet, parent, child3));    
    Assert.assertTrue(multi.isConnectedKeys(velvet, "key30", 25));
    Assert.assertTrue(multi.isConnectedKeys(velvet, "key30", 77));
    Assert.assertTrue(multi.isConnectedKeys(velvet, "key30", 99));
    Assert.assertFalse(multi.isConnectedKeys(velvet, "key30", 80));
    
    List<TestEnt2> multis = multi.multi(velvet, parent);
    Assert.assertEquals(3, multis.size());
    Assert.assertTrue(multis.contains(child1));
    Assert.assertTrue(multis.contains(child2));
    Assert.assertTrue(multis.contains(child3));
  }

  private void testMultiLinkDisconnect(IMultiLinkDef<String, TestEnt, Integer, TestEnt2> multi) {
    fillEntities();
    
    TestEnt parent = new TestEnt("key30", 0.03f);
    TestEnt2 child1 = new TestEnt2(25);
    TestEnt2 child2 = new TestEnt2(77);
    TestEnt2 child3 = new TestEnt2(99);
        
    
    multi.connect(velvet, parent, child1);
    multi.connect(velvet, parent, child2);
    multi.connect(velvet, parent, child3);
    
    multi.disconnect(velvet, parent, child2);
    
    Assert.assertTrue(multi.isConnected(velvet, parent, child1));
    Assert.assertFalse(multi.isConnected(velvet, parent, child2));
    Assert.assertTrue(multi.isConnected(velvet, parent, child3));    
    Assert.assertTrue(multi.isConnectedKeys(velvet, "key30", 25));
    Assert.assertFalse(multi.isConnectedKeys(velvet, "key30", 77));
    Assert.assertTrue(multi.isConnectedKeys(velvet, "key30", 99));
    Assert.assertFalse(multi.isConnectedKeys(velvet, "key30", 80));
    
    List<TestEnt2> multis = multi.multi(velvet, parent);
    Assert.assertEquals(2, multis.size());
    Assert.assertTrue(multis.contains(child1));
    Assert.assertFalse(multis.contains(child2));
    Assert.assertTrue(multis.contains(child3));
  }

  private void testMultiLinkEmpty(IMultiLinkDef<String, TestEnt, Integer, TestEnt2> multi) {
    fillEntities();
    
    TestEnt parent = new TestEnt("key30", 0.03f);
    TestEnt2 child1 = new TestEnt2(25);
    TestEnt2 child2 = new TestEnt2(77);
    TestEnt2 child3 = new TestEnt2(99);
        
    multi.connect(velvet, parent, child1);
    multi.connect(velvet, parent, child2);
    multi.connect(velvet, parent, child3);
    
    multi.disconnect(velvet, parent, child1);
    multi.disconnect(velvet, parent, child2);
    multi.disconnect(velvet, parent, child3);
    
    List<TestEnt2> multis = multi.multi(velvet, parent);
    Assert.assertTrue(multis.isEmpty());
  }
  
  private void testMultiLinkOverwrite(IMultiLinkDef<String, TestEnt, Integer, TestEnt2> multi) {
    fillEntities();

    TestEnt parent = new TestEnt("key30", 0.03f);
    TestEnt2 child1 = new TestEnt2(25);
    TestEnt2 child2 = new TestEnt2(77);
    TestEnt2 child3 = new TestEnt2(99);
        
    multi.connect(velvet, parent, child1);
    multi.connect(velvet, parent, child2);
    multi.connect(velvet, parent, child3);
    multi.connect(velvet, parent, child3);
    multi.connect(velvet, parent, child3);
    
    List<TestEnt2> multis = multi.multi(velvet, parent);
    Assert.assertEquals(3, multis.size());

  }
 
}
