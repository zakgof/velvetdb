package com.zakgof.db.velvet.dynamodb.test;

import org.junit.BeforeClass;

import com.zakgof.db.velvet.test.VelvetTestSuite;

public class DynamoDBVelvetTestSuite extends VelvetTestSuite {

    @BeforeClass
    public static void setup() {
        setup("dynamodb");
    }
}
