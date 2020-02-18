package com.zakgof.db.velvet.dynamodb.test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.text.StrSubstitutor;
import org.junit.BeforeClass;
import org.junit.Ignore;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.VelvetFactory;
import com.zakgof.db.velvet.test.VelvetTestSuite;

@Ignore
public class DynamoDBCachingVelvetTestSuite extends VelvetTestSuite {

    @BeforeClass
    public static void setup() {
        setup(DynamoDBCachingVelvetTestSuite::createEnv, DynamoDBCachingVelvetTestSuite::destroyEnv);
    }

    private static IVelvetEnvironment createEnv() {
        try {
            String url = StrSubstitutor.replaceSystemProperties("velvetdb://dynamodb/us-west-2?awsAccessKeyId=${velvetdb.aws.accessKeyId}&awsSecretKey=${velvetdb.aws.secretKey}&proxyHost=${velvetdb.proxyHost}&proxyPort=${velvetdb.proxyPort}&proxyUser=${velvetdb.proxyUser}&proxyPassword=${velvetdb.proxyPassword}");
            IVelvetEnvironment env = VelvetFactory.openCaching(url);
            env.execute(velvet -> clean(velvet, false));
            return env;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
            return null;
        }
    }

    private static void clean(IVelvet velvet, boolean full) throws NoSuchFieldException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        Field proxyField = velvet.getClass().getDeclaredField("proxy");
        proxyField.setAccessible(true);
        Object rootVelvet = proxyField.get(velvet);
        rootVelvet.getClass().getDeclaredMethod("killAll", boolean.class).invoke(rootVelvet, full);
    }

    private static void destroyEnv(IVelvetEnvironment env) {
        env.execute(velvet -> clean(velvet, true));
        env.close();
    }
}
