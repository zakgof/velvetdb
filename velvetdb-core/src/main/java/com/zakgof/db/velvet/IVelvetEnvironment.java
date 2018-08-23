package com.zakgof.db.velvet;

import java.util.function.Supplier;

import com.zakgof.db.txn.ITransactionalEnvironment;
import com.zakgof.db.velvet.upgrader.IVelvetUpgrader;
import com.zakgof.serialize.ISerializer;

/**
 * Velvet environment wraps an open database handle. Velvet environment provides velvet handles for transactions.
 */
public interface IVelvetEnvironment extends ITransactionalEnvironment<IVelvet>, AutoCloseable {

    /**
     * Close the handle freeing all the resources.
     */
    @Override
    void close(); // Not throwing anything

    /**
     * Set serializer factory.
     *
     * @param serializer serializer factory
     */
    void setSerializer(Supplier<ISerializer> serializer);

    /**
     * Get class version upgrader.
     *
     * @return upgrader interface
     */
    IVelvetUpgrader upgrader();
}
