package com.zakgof.db.velvet.test;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.zakgof.db.velvet.IVelvetEnvironment;

@RunWith(Suite.class)

@SuiteClasses({

    SecondarySortedLinkTest.class,
    SortedStoreIndexesTest.class,
    StoreIndexesAnnoTest.class,
    StoreIndexesTest.class,
    SecondarySortedLinkTest.class,
    PrimarySortedLinkTest.class,
    PrimarySortedLinkTest2.class,
    SortedStoreTest.class,
    SimpleLinkTest.class,
    KeylessTest.class,
    PutGetTest.class,
    UpgradeTest.class,
    JoinTest.class

//    ConcurrentWriteTest.class,
//    PerformanceTest.class,


})

public abstract class VelvetTestSuite {

    private static Consumer<IVelvetEnvironment> destructor;
    private static IVelvetEnvironment env;

    static Supplier<IVelvetEnvironment> velvetProvider;

    protected static void setup(Supplier<IVelvetEnvironment> constructor, Consumer<IVelvetEnvironment> destructor) {
        VelvetTestSuite.destructor = destructor;
        velvetProvider = () -> {
            env = constructor.get();
            return env;
        };
    }

    @AfterClass
    public static void tearDownClass() {
        destructor.accept(env);
        env = null;
    }

}
