package com.zakgof.db.velvet.test;

import com.zakgof.db.velvet.IVelvetEnvironment;

abstract class AVelvetTest {

    protected IVelvetEnvironment env;

    protected AVelvetTest() {
        env = VelvetTestSuite.velvetProvider.get();
    }

}
