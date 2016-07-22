package com.zakgof.db.velvet.mapdb;

import java.io.File;
import java.util.function.Supplier;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.zakgof.db.txn.ITransactionCall;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.serialize.ISerializer;

public class MapDbNoTxnEnv implements IVelvetEnvironment {

    private DB db;
    private Supplier<ISerializer> serializerSupplier;

    public MapDbNoTxnEnv(File dir) {
        File file = new File(dir, "velvet");
        db = DBMaker.fileDB(file.getAbsoluteFile()).closeOnJvmShutdown().make();
    }

    @Override
    public void execute(ITransactionCall<IVelvet> transaction) {
        try {
            transaction.execute(new MapDbVelvet(db, serializerSupplier));
        } catch (Throwable e) {
            throw new VelvetException(e);
        }
    }

    @Override
    public void close() {
        db.close();
    }

    @Override
    public void setSerializer(Supplier<ISerializer> serializer) {
        this.serializerSupplier = serializer;
    }

}
