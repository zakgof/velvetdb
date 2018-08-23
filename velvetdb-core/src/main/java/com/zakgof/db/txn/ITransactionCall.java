package com.zakgof.db.txn;

/**
 * Transaction without a return value.
 *
 * @param <H> transaction handle type
 */
@FunctionalInterface
public interface ITransactionCall<H> {

    /**
     * Execute a transaction without a return value.
     *
     * @param handle transaction handle
     * @throws Throwable
     */
    public void execute(H handle) throws Throwable;

}
