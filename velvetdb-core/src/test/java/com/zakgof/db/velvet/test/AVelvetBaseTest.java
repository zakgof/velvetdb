package com.zakgof.db.velvet.test;

import java.io.File;
import java.nio.file.Files;

import org.junit.AfterClass;
import org.junit.Assert;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.VelvetFactory;

class AVelvetBaseTest {

    private static String defaultProviderPath;

    protected AVelvetBaseTest() {
        if (VelvetTestSuite.velvetProvider == null) {
            if (!VelvetFactory.getProviders().stream().anyMatch(provider -> provider.name().equals("xodus"))) {
                Assert.fail("If you run tests not using a concrete VelvetTestSuite implementation, please add velvetdb-xodus to the classpath");
            }
            VelvetTestSuite.velvetProvider = this::createDefaultEnv;
        }
    }

    private IVelvetEnvironment createDefaultEnv() {
        try {
            defaultProviderPath = Files.createTempDirectory("velvet").toString();
            new File(defaultProviderPath).mkdirs();
            IVelvetEnvironment env = VelvetFactory.open("velvetdb://xodus/" + defaultProviderPath.replace(File.separatorChar, '/'));
            return env;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
            return null;
        }

    }

    private static void destroyEnv(IVelvetEnvironment env, String path) {
        if (env != null) {
            env.close();
            for (File file : new File(path).listFiles()) {
               file.delete();
            }
        }
    }

    @AfterClass
    public static void destroyDefaultEnv() {
        if (defaultProviderPath != null) {
            destroyEnv(VelvetTestSuite.velvetProvider.get(), defaultProviderPath);
            VelvetTestSuite.velvetProvider = null;
        }
    }

}
