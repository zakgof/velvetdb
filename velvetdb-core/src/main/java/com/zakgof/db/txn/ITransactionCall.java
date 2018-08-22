package com.zakgof.db.txn;

/**
 * Transaction without a result.
 *
 * @param <H> database connection handle type
 */
@FunctionalInterface
public interface ITransactionCall<H> {

    public void execute(H handle) throws Throwable;

}
