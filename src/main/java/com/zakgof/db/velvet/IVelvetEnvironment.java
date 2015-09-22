package com.zakgof.db.velvet;

import com.zakgof.db.txn.ITransactionalEnvironment;

public interface IVelvetEnvironment extends ITransactionalEnvironment<IVelvet>, AutoCloseable {
  void close();
}
