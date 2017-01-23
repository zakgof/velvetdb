package com.zakgof.db.velvet.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Supplier;

import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.VelvetFactory;

@RunWith(Suite.class)

@SuiteClasses({ StoreIndexesTest.class, SecondarySortedLinkTest.class, SortedStoreTest.class, PutGetTest.class, SimpleLinkTest.class, PrimarySortedLinkTest.class, PrimarySortedLinkTest2.class, KeylessTest.class
        // PerformanceTest.class,
        // ConcurrentWriteTest.class,

})

public abstract class VelvetTestSuite {

    protected static Supplier<IVelvetEnvironment> velvetProvider;
    private static IVelvetEnvironment env;
    private static String PATH;

    protected static void setup(String providerName) {
        try {
            PATH = Files.createTempDirectory("velvet").toString();
        } catch (IOException e) {
            throw new VelvetException(e);
        }
        velvetProvider = () -> createVelvet(providerName);
    }

    private static IVelvetEnvironment createVelvet(String providerName) {
        tearDownClass();
        new File(PATH).mkdirs();
        env = VelvetFactory.open("velvetdb://" + providerName + "/" + PATH.replace(File.separatorChar, '/'));
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
