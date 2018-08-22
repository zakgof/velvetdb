package com.zakgof.db.txn;

/**
 * Transaction that returns a result.
 *
 * @param <H> database connection handle type
 * @param <R> transaction result type
 */
@FunctionalInterface
public interface ITransactionCalc<H, R> {

    public R execute(H handle) throws Throwable;

}
