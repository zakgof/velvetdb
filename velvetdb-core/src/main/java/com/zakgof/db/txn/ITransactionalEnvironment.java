package com.zakgof.db.txn;

import com.zakgof.db.velvet.VelvetException;

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
    @SuppressWarnings("unchecked")
    public default <R> R calculate(ITransactionCalc<H, R> transaction) {
        Object[] result = new Object[1];
        try {
            execute(h -> result[0] = transaction.execute(h));
        } catch (Exception e) {
            throw ((e instanceof RuntimeException) ? (RuntimeException) e : new VelvetException(e));
        }
        return (R) result[0];
    }

}
