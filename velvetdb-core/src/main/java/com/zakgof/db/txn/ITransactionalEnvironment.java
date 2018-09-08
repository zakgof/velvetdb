package com.zakgof.db.txn;

/**
 * Environment for running transactions.
 *
 * @param <H> transaction handle type
 */
public interface ITransactionalEnvironment<H> {

    /**
     * Run a transaction.
     *
     * @param transaction transaction
     */
    public void execute(ITransactionCall<H> transaction);

    /**
     * Run a transaction and return a result.
     *
     * @param transaction transaction
     * @param <R> transaction result type
     * @return transaction result
     */
    public <R> R calculate(ITransactionCalc<H, R> transaction);

}
