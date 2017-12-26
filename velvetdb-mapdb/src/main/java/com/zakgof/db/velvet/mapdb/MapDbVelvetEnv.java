package com.zakgof.db.velvet.mapdb;

import java.io.File;
import java.util.function.Supplier;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DBMaker.Maker;
import org.mapdb.TxMaker;

import com.zakgof.db.txn.ITransactionCall;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.impl.AVelvetEnvironment;
import com.zakgof.serialize.ISerializer;

public class MapDbVelvetEnv extends AVelvetEnvironment {

    private TxMaker txMaker;
    private Supplier<ISerializer> serializerSupplier;

    public MapDbVelvetEnv(File dir) {
        File file = new File(dir, "velvet");
        txMaker = DBMaker.fileDB(file.getAbsoluteFile()).closeOnJvmShutdown().makeTxMaker();
    }

    public MapDbVelvetEnv(Maker maker) {
        this(maker.makeTxMaker());
    }

    public MapDbVelvetEnv(TxMaker txMaker) {
        this.txMaker = txMaker;
    }

    @Override
    public void execute(ITransactionCall<IVelvet> transaction) {
        DB db = txMaker.makeTx();
        try {
            transaction.execute(new MapDbVelvet(db, serializerSupplier));
            db.commit();
        } catch (Throwable e) {
            db.rollback();
            throw (e instanceof RuntimeException) ? (RuntimeException)e : new VelvetException(e);
        } finally {
            db.close();
        }
    }

    @Override
    public void close() {
        txMaker.close();
    }
}
