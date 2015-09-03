package com.zakgof.db.velvet;

import txn.ITransactionalEnvironment;

public interface IVelvetEnvironment extends ITransactionalEnvironment<IVelvet>, AutoCloseable {
  void close();
}
