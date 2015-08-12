package com.zakgof.db.velvet.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.entity.impl.Entities;
import com.zakgof.db.velvet.api.link.IBiManyToManyLinkDef;
import com.zakgof.db.velvet.api.link.IBiMultiLinkDef;
import com.zakgof.db.velvet.api.link.IBiSingleLinkDef;
import com.zakgof.db.velvet.api.link.ISingleLinkDef;
import com.zakgof.db.velvet.api.link.Links;

public class SimpleLinkTest {

  private static final int COUNT = 1000;
  private IVelvet velvet;

  private IEntityDef<String, TestEnt> ENTITY = Entities.create(TestEnt.class);
  private IEntityDef<Integer, TestEnt2> ENTITY2 = Entities.create(TestEnt2.class);

  private ISingleLinkDef<String, TestEnt, Integer, TestEnt2> SINGLE = Links.single(ENTITY, ENTITY2, "single");
  private ISingleLinkDef<String, TestEnt, Integer, TestEnt2> MULTI = Links.single(ENTITY, ENTITY2, "multi");

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
    fillEntities();

    TestEnt e = new TestEnt("key13", 0.013f);
    TestEnt2 e2 = new TestEnt2(99);

    TestEnt xe = new TestEnt("key23", 0.023f);
    TestEnt2 xe2 = new TestEnt2(77);

    TestEnt ze = new TestEnt("key29", 0.029f);

    SINGLE.connect(velvet, e, e2);
    SINGLE.connect(velvet, xe, xe2);

    TestEnt2 t2 = SINGLE.single(velvet, e);
    Assert.assertNotNull(t2);
    Assert.assertEquals("v99", t2.getVal());

    Integer t2key = SINGLE.singleKey(velvet, "key23");
    Assert.assertEquals(new Integer(77), t2key);

    TestEnt2 tz = SINGLE.single(velvet, ze);
    Assert.assertNull(tz);
    Integer tzkey = SINGLE.singleKey(velvet, "key56");
    Assert.assertNull(tzkey);
  }

  private void fillEntities() {
    for (int d = 0; d < COUNT; d++) {
      TestEnt e = new TestEnt("key" + d, d * 0.001f);
      ENTITY.put(velvet, e);
      TestEnt2 e2 = new TestEnt2(d);
      ENTITY2.put(velvet, e2);
    }
  }
}
