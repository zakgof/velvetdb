package com.zakgof.db.velvet.test;

import java.io.File;
import java.util.function.Supplier;

import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.VelvetFactory;

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

public abstract class VelvetTestSuite {

    protected static Supplier<IVelvetEnvironment> velvetProvider;
    private static IVelvetEnvironment env;
    private static String PATH = "D:/Pr/lab/xodustest"; // TODO

    protected static void setup(String providerName) {
        velvetProvider = () -> createVelvet(providerName);
    }

    private static IVelvetEnvironment createVelvet(String providerName) {
        tearDownClass();
        env = VelvetFactory.open("velvetdb://" + providerName + "/" + PATH);
        // env.setSerializer(() -> new KryoSerializer());
        // env.setSerializer(() -> new ElsaVelvetSerializer());
        return env;
    }

    @AfterClass
    public static void tearDownClass() {
        if (env != null) {
            env.close();
            for (File file : new File(PATH).listFiles())
                file.delete();
        }
    }
}
