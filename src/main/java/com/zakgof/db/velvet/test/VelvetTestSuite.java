package com.zakgof.db.velvet.test;

import java.util.function.Supplier;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.zakgof.db.velvet.IVelvet;

@RunWith(Suite.class)
// @SuiteClasses({PutGetTest.class, SimpleLinkTest.class, PrimaryIndexTest.class, SecondaryIndexTest.class, SortedStoreTest.class})
@SuiteClasses({ConcurrentWriteTest.class})

public abstract class VelvetTestSuite {
  
  public static Supplier<IVelvet> velvetProvider;
  
  public static Supplier<IVelvet> velvetTxnProvider;
  
}
