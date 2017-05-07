package com.zakgof.db.velvet.xodus.test;

import org.junit.BeforeClass;

import com.zakgof.db.velvet.test.VelvetTestSuite;

public class XodusVelvetTestSuite extends VelvetTestSuite {

    @BeforeClass
    public static void setup() {
        setup("xodus");
    }
}
