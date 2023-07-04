package com.zakgof.velvet.request;

import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.IVelvetReadTransaction;

public interface IReadCommand<T>  {

    T execute(IVelvetReadTransaction readTxn);

    default T execute(IVelvetEnvironment velvetEnv) {
        return velvetEnv.txnRead(this::execute);
    }

}
