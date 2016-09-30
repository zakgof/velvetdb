package com.zakgof.db.velvet.xodus;

import java.io.File;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.zakgof.db.txn.ITransactionCall;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.serialize.ISerializer;
import com.zakgof.serialize.ZeSerializer;

import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Environments;

public class XodusVelvetEnv implements IVelvetEnvironment {

  private Environment env;
  private IKeyGen keyGen;
  private Supplier<ISerializer> serializerSupplier;

  public XodusVelvetEnv(File file) {
    env = Environments.newInstance(file.getAbsolutePath());
    keyGen = new IKeyGen();
    serializerSupplier = () -> new ZeSerializer(ImmutableMap.of(ZeSerializer.USE_OBJENESIS, true));
  }

  @Override
  public void execute(ITransactionCall<IVelvet> transaction) {
    final Throwable[] exs = new Throwable[1];
    final int[] count = new int[]{0};
    env.executeInTransaction(txn -> {
      try {
    	  if (count[0] > 0)
    		  System.err.println("Xodus transaction retry " + count[0]); // TODO
        transaction.execute(new XodusVelvet(env, txn, keyGen, serializerSupplier));
        count[0]++;
      } catch (Throwable e) {
        exs[0] = e;
      }
    });
    if (exs[0] != null)
      throw (exs[0] instanceof RuntimeException) ? (RuntimeException)exs[0] : new VelvetException(exs[0]);
  }

  @Override
  public void close() {
    env.close();
  }

  @Override
  public void setSerializer(Supplier<ISerializer> serializerSupplier) {
    this.serializerSupplier = serializerSupplier;
  }
  
}
