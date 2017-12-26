package com.zakgof.db.velvet.mapdb;

import java.io.File;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.zakgof.db.txn.ITransactionCall;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.impl.AVelvetEnvironment;

public class MapDbNoTxnEnv extends AVelvetEnvironment {

    private DB db;

    public MapDbNoTxnEnv(File dir) {
        File file = new File(dir, "velvet");
        db = DBMaker.fileDB(file.getAbsoluteFile()).closeOnJvmShutdown().make();
    }

    @Override
    public void execute(ITransactionCall<IVelvet> transaction) {
        try {
            transaction.execute(new MapDbVelvet(db, this::instantiateSerializer));
        } catch (Throwable e) {
            throw new VelvetException(e);
        }
    }

    @Override
    public void close() {
        db.close();
    }

}
