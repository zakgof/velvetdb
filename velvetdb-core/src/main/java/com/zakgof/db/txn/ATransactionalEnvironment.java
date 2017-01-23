package com.zakgof.db.txn;

public abstract class ATransactionalEnvironment<H> implements ITransactionalEnvironment<H> {

    @SuppressWarnings("unchecked")
    public <R> R calculate(ITransactionCalc<H, R> transaction) {
        Object[] result = new Object[1];
        execute(h -> result[0] = transaction.execute(h));
        return (R) result[0];
    }
}
