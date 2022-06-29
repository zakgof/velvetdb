package com.zakgof.velvet.test;

import com.zakgof.velvet.IVelvetEnvironment;

public interface ITestVelvetEnvProvider extends AutoCloseable{
    IVelvetEnvironment create();

    void close();
}
