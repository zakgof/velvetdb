package com.zakgof.db.velvet.kvs;

import org.junit.BeforeClass;

import com.zakgof.db.test.VelvetTestSuite;
import com.zakgof.db.txn.ITransactionCall;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvetEnvironment;

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