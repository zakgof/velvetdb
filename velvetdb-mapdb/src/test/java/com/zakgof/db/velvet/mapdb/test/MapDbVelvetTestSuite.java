package com.zakgof.db.velvet.mapdb.test;

import org.junit.BeforeClass;

import com.zakgof.db.velvet.test.VelvetTestSuite;

public class MapDbVelvetTestSuite extends VelvetTestSuite {

    @BeforeClass
    public static void setup() {
        setup("mapdb");
    }
}
