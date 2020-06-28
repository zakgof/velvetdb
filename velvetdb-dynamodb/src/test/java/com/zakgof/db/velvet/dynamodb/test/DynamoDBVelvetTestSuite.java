package com.zakgof.db.velvet.dynamodb.test;

import org.apache.commons.text.StrSubstitutor;
import org.junit.BeforeClass;
import org.junit.Ignore;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.VelvetFactory;
import com.zakgof.db.velvet.test.VelvetTestSuite;

@Ignore
public class DynamoDBVelvetTestSuite extends VelvetTestSuite {

    @BeforeClass
    public static void setup() {
        setup(DynamoDBVelvetTestSuite::createEnv, DynamoDBVelvetTestSuite::destroyEnv);
    }

    private static IVelvetEnvironment createEnv() {
        try {
            String url = StrSubstitutor.replaceSystemProperties("velvetdb://dynamodb/us-west-2?awsAccessKeyId=${velvetdb.aws.accessKeyId}&awsSecretKey=${velvetdb.aws.secretKey}&proxyHost=${velvetdb.proxyHost}&proxyPort=${velvetdb.proxyPort}&proxyUser=${velvetdb.proxyUser}&proxyPassword=${velvetdb.proxyPassword}");
            IVelvetEnvironment env = VelvetFactory.open(url);
            env.execute(velvet -> velvet.getClass().getDeclaredMethod("killAll", boolean.class).invoke(velvet, false));
            return env;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
            return null;
        }
    }

    private static void destroyEnv(IVelvetEnvironment env) {
        env.execute(velvet -> velvet.getClass().getDeclaredMethod("killAll", boolean.class).invoke(velvet, true));
        env.close();
    }
}
