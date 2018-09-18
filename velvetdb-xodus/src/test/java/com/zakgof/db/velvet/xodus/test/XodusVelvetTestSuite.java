package com.zakgof.db.velvet.xodus.test;

import java.io.File;
import java.nio.file.Files;

import org.junit.BeforeClass;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.VelvetFactory;
import com.zakgof.db.velvet.test.VelvetTestSuite;

public class XodusVelvetTestSuite extends VelvetTestSuite {

    private static String PATH;

    @BeforeClass
    public static void setup() {
        setup(XodusVelvetTestSuite::createEnv, XodusVelvetTestSuite::destroyEnv);
    }

    private static IVelvetEnvironment createEnv() {
        try {
            PATH = Files.createTempDirectory("velvet").toString();
            new File(PATH).mkdirs();
            IVelvetEnvironment env = VelvetFactory.open("velvetdb://xodus/" + PATH.replace(File.separatorChar, '/'));
            // env.setSerializer(() -> new KryoSerializer());
            return env;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
            return null;
        }

    }

    private static void destroyEnv(IVelvetEnvironment env) {
        if (env != null) {
            env.close();
            for (File file : new File(PATH).listFiles()) {
               file.delete();
            }
        }
    }
}
