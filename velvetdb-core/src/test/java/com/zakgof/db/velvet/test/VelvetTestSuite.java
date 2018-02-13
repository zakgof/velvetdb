package com.zakgof.db.velvet.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Supplier;

import org.apache.commons.text.StrSubstitutor;
import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.VelvetFactory;

@RunWith(Suite.class)

@SuiteClasses({

    StoreIndexesTest.class,

    /*

    SecondarySortedLinkTest.class,
    PrimarySortedLinkTest.class,
    PrimarySortedLinkTest2.class,
    SortedStoreTest.class,
    SimpleLinkTest.class,
    KeylessTest.class,
    PutGetTest.class,


    ConcurrentWriteTest.class,
    PerformanceTest.class,

    */
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
        cleanup(false);
        if (providerName.equals("datastore")) {
            String url =  StrSubstitutor.replaceSystemProperties("velvetdb://datastore/${velvetdb.datastore.projectId}/?credentialPath=${velvetdb.datastore.credentialPath}&proxyHost=${velvetdb.proxyHost}&proxyPort=${velvetdb.proxyPort}&proxyUser=${velvetdb.proxyUser}&proxyPassword=${velvetdb.proxyPassword}");
            env = VelvetFactory.open(url);
        } else if (providerName.equals("dynamodb")) {
            if (env == null) {
                String url =  StrSubstitutor.replaceSystemProperties("velvetdb://dynamodb/us-west-2?awsAccessKeyId=${velvetdb.aws.accessKeyId}&awsSecretKey=${velvetdb.aws.secretKey}"); // &proxyHost=${velvetdb.proxyHost}&proxyPort=${velvetdb.proxyPort}&proxyUser=${velvetdb.proxyUser}&proxyPassword=${velvetdb.proxyPassword}");            )
                env = VelvetFactory.open(url);
                env.execute(velvet -> {
                    velvet.getClass().getDeclaredMethod("killAll", boolean.class).invoke(velvet, false);
                });
            }
        } else{
            new File(PATH).mkdirs();
            env = VelvetFactory.open("velvetdb://" + providerName + "/" + PATH.replace(File.separatorChar, '/'));
            // env.setSerializer(() -> new KryoSerializer());
            // env.setSerializer(() -> new ElsaVelvetSerializer());
        }
        return env;
    }

    @AfterClass
    public static void tearDownClass() {
        cleanup(true);
    }

    private static void cleanup(boolean full) {
        if (env != null) {
            if (isDynamo()) {
                env.execute(velvet -> velvet.getClass().getDeclaredMethod("killAll", boolean.class).invoke(velvet, full));

            } else {
                env.close();
            }
        }

        if (!isDynamo()) {
            for (File file : new File(PATH).listFiles())
                file.delete();
        }
    }

    private static boolean isDynamo() {
        return env != null && env.getClass().getName().toLowerCase().contains("dynamo");
    }
}
