package com.zakgof.velvet;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Velvet environment wraps an open database handle. Velvet environment provides velvet handles for transactions.
 */
public interface IVelvetEnvironment extends AutoCloseable {

    void txnWrite(Consumer<IVelvetWriteTransaction> action);

    <R> R txnRead(Function<IVelvetReadTransaction, R> action);

    /**
     * Close the handle freeing all the resources.
     */
    @Override
    void close(); // Not throwing anything
}
