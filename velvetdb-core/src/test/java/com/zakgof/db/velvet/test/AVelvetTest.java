package com.zakgof.db.velvet.test;

import com.zakgof.db.velvet.IVelvetEnvironment;

abstract class AVelvetTest extends AVelvetBaseTest {

    protected IVelvetEnvironment env;

    protected AVelvetTest() {
        super();
        env = VelvetTestSuite.velvetProvider.get();
    }

}
