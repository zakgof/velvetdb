package com.zakgof.db.velvet.kvs;

import org.junit.BeforeClass;

import com.zakgof.db.sqlkvs.MemKvs;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.test.VelvetTestSuite;

public class MemKvsVelvetSuite extends VelvetTestSuite {

  @BeforeClass 
  public static void setUpClass() {
    velvetProvider = () -> createVelvet();  
  }

  private static IVelvet createVelvet() {
    MemKvs kvs = new MemKvs();
    return new GenericKvsVelvet3(kvs);
  }

}