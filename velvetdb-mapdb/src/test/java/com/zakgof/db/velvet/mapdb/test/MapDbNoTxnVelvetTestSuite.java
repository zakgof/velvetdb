package com.zakgof.db.velvet.mapdb.test;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.mapdb.MapDbNoTxnEnv;
import com.zakgof.db.velvet.test.VelvetTestSuite;

public class MapDbNoTxnVelvetTestSuite extends VelvetTestSuite {

  private static File PATH = new File("D:\\Pr\\mapdbtest");
  private static MapDbNoTxnEnv env;

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
    env = new MapDbNoTxnEnv(new File(PATH, "mapdb"));
    return env;
  }
}
