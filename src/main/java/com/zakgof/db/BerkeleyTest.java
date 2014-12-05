package com.zakgof.db;

import java.io.File;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BerkeleyTest {

  public static void main(String[] args) {
    
    
    try {
      EnvironmentConfig envConf = new EnvironmentConfig();
      envConf.setAllowCreate(true);
      Environment dbEnv = new Environment(new File("D:\\Pr\\berkeley"), envConf);
      
      DatabaseConfig dbConf = new DatabaseConfig();
      dbConf.setAllowCreate(true);
      Database db = dbEnv.openDatabase(null, "test", dbConf);
      
      

  } catch (DatabaseException dbe) {
      System.out.println("Error :" + dbe.getMessage() );
  }
    

  }

}
