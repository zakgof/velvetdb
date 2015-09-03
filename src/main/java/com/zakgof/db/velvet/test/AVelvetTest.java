package com.zakgof.db.velvet.test;

import org.junit.runner.RunWith;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvetEnvironment;

@RunWith(VelvetTransactionalRunner.class)
public class AVelvetTest {
  
  protected IVelvetEnvironment velvetEnv;
  
  public IVelvet velvet;

}
