package com.zakgof.db.velvet.kvs;

import com.zakgof.db.sqlkvs.MemKvs;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.test.VelvetTest;

public class MemKvsVelvetTest extends VelvetTest {

  @Override
  protected IVelvet createVelvet() {
    MemKvs kvs = new MemKvs();
    return new GenericKvsVelvet3(kvs);
  }

}
