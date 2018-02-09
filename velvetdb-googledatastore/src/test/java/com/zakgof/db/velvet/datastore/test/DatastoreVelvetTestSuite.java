package com.zakgof.db.velvet.datastore.test;

import org.junit.BeforeClass;

import com.zakgof.db.velvet.test.VelvetTestSuite;

public class DatastoreVelvetTestSuite extends VelvetTestSuite {

    @BeforeClass
    public static void setup() {
        setup("datastore");
    }
}
