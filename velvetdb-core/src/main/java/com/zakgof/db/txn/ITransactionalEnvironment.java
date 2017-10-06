package com.zakgof.db.txn;

public interface ITransactionalEnvironment<H> {

    public void execute(ITransactionCall<H> transaction);

    public <R> R calculate(ITransactionCalc<H, R> transaction);

}
