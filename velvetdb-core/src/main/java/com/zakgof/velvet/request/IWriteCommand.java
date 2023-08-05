package com.zakgof.velvet.request;

import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.IVelvetWriteTransaction;

public interface IWriteCommand {

    void execute(IVelvetWriteTransaction writeTxn);

    default void execute(IVelvetEnvironment velvetEnv) {
        velvetEnv.txnWrite(this::execute);
    }

}
