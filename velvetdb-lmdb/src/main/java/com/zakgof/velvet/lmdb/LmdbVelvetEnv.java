package com.zakgof.velvet.lmdb;

import com.zakgof.velvet.ISerializerProvider;
import com.zakgof.velvet.IVelvet;
import com.zakgof.velvet.IVelvetEnvironment;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ServiceLoader;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

public class LmdbVelvetEnv implements IVelvetEnvironment {

    private final Env<ByteBuffer> env;
    private final ISerializerProvider serializerProvider;

    public static void main(String[] args) {

        Env<ByteBuffer> envir = create()
                .setMapSize(100_000_000) // TODO
                .setMaxDbs(1024) // TODO
                .open(new File("E:\\Business\\lmdblab"));

        try (Txn<ByteBuffer> txn = envir.txnWrite()) {
            Dbi<ByteBuffer> dbi = envir.openDbi("somecarze", MDB_CREATE);
            txn.commit();
        }

        try (Txn<ByteBuffer> txn = envir.txnWrite()) {

            Dbi<ByteBuffer> dbi = envir.openDbi("somecarze", MDB_CREATE);
            final ByteBuffer bb1 = ByteBuffer.allocateDirect(8).putInt(10).flip();
            dbi.put(txn, bb1, bb1);
            txn.commit();
        }
    }

    public LmdbVelvetEnv(File path) {

        path.mkdirs();

        env = create()
                .setMapSize(100_000_000) // TODO
                .setMaxDbs(1024) // TODO
                .open(path);

        serializerProvider = ServiceLoader.load(ISerializerProvider.class)
                .findFirst()
                .get();
    }

    @Override
    public IVelvet velvet() {
        return new LmdbVelvet(env, serializerProvider.get());
    }

    @Override
    public <R> R txnRead(Function<IVelvet, R> action) {
        return action.apply(velvet());
        /*
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            return action.apply(new LmdbVelvet(env, txn, serializerProvider.get()));
        }
         */
    }

    @Override
    public void txnWrite(Consumer<IVelvet> action) {
        action.accept(velvet());
        /*
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            action.accept(new LmdbVelvet(env, txn, serializerProvider.get()));
            txn.commit();
        }
        */
    }

    @Override
    public void close() {
        env.close();
    }

}
