package com.zakgof.db.velvet.test;

import java.util.function.Supplier;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.zakgof.db.velvet.IVelvetEnvironment;

@RunWith(Suite.class)

 @SuiteClasses({
   StoreIndexesTest.class,
   PutGetTest.class,
   SimpleLinkTest.class,
   PrimarySortedLinkTest.class,
   PrimarySortedLinkTest2.class,
   SecondarySortedLinkTest.class,
   SortedStoreTest.class,
   PerformanceTest.class,
   ConcurrentWriteTest.class,
   KeylessTest.class
})


// @SuiteClasses({KeylessTest.class})

public abstract class VelvetTestSuite {
  
  public static Supplier<IVelvetEnvironment> velvetProvider;
  
  
}
