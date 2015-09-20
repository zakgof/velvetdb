package com.zakgof.db.velvet;

import com.zakgof.db.velvet.txn.ITransactionalEnvironment;

public interface IVelvetEnvironment extends ITransactionalEnvironment<IVelvet>, AutoCloseable {
  void close();
}
