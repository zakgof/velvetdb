package com.zakgof.db.velvet.test;

import org.junit.runner.RunWith;

import com.zakgof.db.velvet.IVelvet;

@RunWith(VelvetTransactionalRunner.class)
public abstract class AVelvetTxnTest {
  
  public IVelvet velvet;

}
