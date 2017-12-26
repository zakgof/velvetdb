package com.zakgof.db.velvet;

import java.util.function.Supplier;

import com.zakgof.db.txn.ITransactionalEnvironment;
import com.zakgof.db.velvet.upgrader.IVelvetUpgrader;
import com.zakgof.serialize.ISerializer;

public interface IVelvetEnvironment extends ITransactionalEnvironment<IVelvet>, AutoCloseable {
    @Override
    void close(); // Not throwing anything

    void setSerializer(Supplier<ISerializer> serializer);

    IVelvetUpgrader upgrader();
}
