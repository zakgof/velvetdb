package com.zakgof.db.velvet.mapdb.test;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.mapdb.MapDbVelvetEnv;
import com.zakgof.db.velvet.test.VelvetTestSuite;

public class MapDbVelvetTestSuite extends VelvetTestSuite {

  private static File PATH = new File("D:/Pr/lab/mapdbtest"); // TODO
  private static MapDbVelvetEnv env;

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
    PATH.mkdirs();
    env = new MapDbVelvetEnv(new File(PATH, "mapdb"));
    return env;
  }
}
