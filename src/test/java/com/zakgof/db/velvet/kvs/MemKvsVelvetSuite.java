package com.zakgof.db.velvet.kvs;

import org.junit.BeforeClass;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.test.VelvetTestSuite;

import txn.ITransactionCall;

public class MemKvsVelvetSuite extends VelvetTestSuite {

  @BeforeClass 
  public static void setUpClass() {
    velvetProvider = () -> createVelvet();  
  }

  private static IVelvetEnvironment createVelvet() {
    return new IVelvetEnvironment() {
      
      @Override
      public void execute(ITransactionCall<IVelvet> transaction) {
        // transaction.execute(new GenericKvsVelvet(new MemKvs()));
      }
      
      @Override
      public void close() {
      }
    };
  }

}