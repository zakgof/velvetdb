package com.zakgof.db.txn;

@FunctionalInterface
public interface ITransactionCalc<H, R> {

    public R execute(H handle) throws Throwable;

}
