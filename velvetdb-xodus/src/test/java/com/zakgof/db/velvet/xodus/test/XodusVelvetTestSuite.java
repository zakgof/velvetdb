package com.zakgof.db.velvet.xodus.test;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.test.VelvetTestSuite;
import com.zakgof.db.velvet.xodus.XodusVelvetEnv;

public class XodusVelvetTestSuite extends VelvetTestSuite {

  private static XodusVelvetEnv env;
  private static File PATH = new File("D:/Pr/lab/xodustest");

  @BeforeClass 
  public static void setUpClass() {
    velvetProvider = () -> createVelvet();
  }

  @AfterClass 
  public static void tearDownClass() {
    if (env != null) {      
      env.close();
      for(File file: PATH.listFiles()) file.delete();
    }
  }

  private static IVelvetEnvironment createVelvet() {
    tearDownClass();
    env = new XodusVelvetEnv(PATH);
    return env;
  }
}
