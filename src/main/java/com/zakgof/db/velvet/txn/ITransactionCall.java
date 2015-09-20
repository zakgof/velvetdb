package com.zakgof.db.velvet.txn;

@FunctionalInterface
public interface ITransactionCall<H> {

  public void execute(H handle) throws Throwable;

}
