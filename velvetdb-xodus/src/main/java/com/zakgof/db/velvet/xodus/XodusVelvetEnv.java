package com.zakgof.db.velvet.xodus;

import java.io.File;

import com.zakgof.db.txn.ITransactionCall;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.impl.AVelvetEnvironment;

import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Environments;

public class XodusVelvetEnv extends AVelvetEnvironment {

    private Environment env;
    private IKeyGen keyGen;

    public XodusVelvetEnv(File file) {
        env = Environments.newInstance(file.getAbsolutePath());
        keyGen = new IKeyGen();
    }

    @Override
    public void execute(ITransactionCall<IVelvet> transaction) {
        final Throwable[] exs = new Throwable[1];
        final int[] count = new int[] { 0 };
        env.executeInTransaction(txn -> {
            try {
                if (count[0] > 0)
                    System.err.println("Xodus transaction retry " + count[0]); // TODO
                transaction.execute(new XodusVelvet(env, txn, keyGen, this::instantiateSerializer));
                count[0]++;
            } catch (Throwable e) {
                exs[0] = e;
            }
        });
        if (exs[0] != null) {
            Throwable e = exs[0];
            if (e instanceof RuntimeException)
                throw (RuntimeException)e;
            else if (e instanceof Error)
                throw (Error)e;
            throw new VelvetException(e);
        }
    }

    @Override
    public void close() {
        env.close();
    }
}
