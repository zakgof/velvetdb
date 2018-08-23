package com.zakgof.db.txn;

/**
 * Transaction that returns a result.
 *
 * @param <H> transaction handle type
 * @param <R> transaction result type
 */
@FunctionalInterface
public interface ITransactionCalc<H, R> {

    /**
     * Execute a transaction that returns a result.
     *
     * @param handle transaction handle
     * @return transaction result
     * @throws Throwable exception during transaction
     */
    public R execute(H handle) throws Throwable;

}
