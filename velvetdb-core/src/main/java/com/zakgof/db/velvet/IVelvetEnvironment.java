package com.zakgof.db.velvet;

import com.annimon.stream.function.Supplier;
import com.zakgof.db.txn.ITransactionalEnvironment;
import com.zakgof.serialize.ISerializer;

public interface IVelvetEnvironment extends ITransactionalEnvironment<IVelvet>, AutoCloseable {
    void close();

    void setSerializer(Supplier<ISerializer> serializer);
}
