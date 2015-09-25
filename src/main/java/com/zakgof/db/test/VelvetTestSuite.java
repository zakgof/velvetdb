package com.zakgof.db.test;

import java.util.function.Supplier;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.zakgof.db.velvet.IVelvetEnvironment;

@RunWith(Suite.class)

 @SuiteClasses({
   PutGetTest.class,
   SimpleLinkTest.class,
   PrimaryIndexTest.class,
   PrimaryIndexTest2.class,
   SecondaryIndexTest.class,
   SortedStoreTest.class,
   PerformanceTest.class,
   ConcurrentWriteTest.class,
   KeylessTest.class
})


// @SuiteClasses({KeylessTest.class})

public abstract class VelvetTestSuite {
  
  public static Supplier<IVelvetEnvironment> velvetProvider;
  
  
}
