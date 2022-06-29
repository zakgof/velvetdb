package com.zakgof.velvet.request;

import com.zakgof.velvet.IVelvet;
import com.zakgof.velvet.IVelvetEnvironment;

public interface IWriteRequest {

    void execute(IVelvet velvet);

    default void execute(IVelvetEnvironment velvetEnv) {
        velvetEnv.txnWrite(this::execute);
    }

}
