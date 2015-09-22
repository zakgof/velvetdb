package com.zakgof.db.test;

import org.junit.Before;

public class PrimaryIndexTest2 extends PrimaryIndexTest {

  @Before
  public void init() {

    TestEnt root2 = new TestEnt("aa", 1.0f);
    ENTITY.put(velvet, root2);

    TestEnt2 e2 = new TestEnt2(-1);
    ENTITY2.put(velvet, e2);
    MULTI.connect(velvet, root2, e2);
    
    super.init();

    TestEnt root3 = new TestEnt("zzzzz", 1.0f);
    ENTITY.put(velvet, root3);

    TestEnt2 e3 = new TestEnt2(10);
    ENTITY2.put(velvet, e3);
    MULTI.connect(velvet, root3, e3);

  }

}
