package com.zakgof.db.txn;

import com.zakgof.db.velvet.VelvetException;

public abstract class ATransactionalEnvironment<H> implements ITransactionalEnvironment<H> {

    @Override
    @SuppressWarnings("unchecked")
    public <R> R calculate(ITransactionCalc<H, R> transaction) {
        Object[] result = new Object[1];
        try {
            execute(h -> result[0] = transaction.execute(h));
        } catch (Throwable e) {
            new VelvetException(e);
        }
        return (R) result[0];
    }

}
