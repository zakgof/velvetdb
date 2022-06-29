package com.zakgof.velvet.request;

import com.zakgof.velvet.IVelvet;
import com.zakgof.velvet.IVelvetEnvironment;

public interface IReadRequest<T>  {

    T execute(IVelvet velvet);

    default T execute(IVelvetEnvironment velvetEnv) {
        return velvetEnv.txnRead(this::execute);
    }

}
