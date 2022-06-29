package com.zakgof.velvet.xodus;

import com.zakgof.velvet.ISerializerProvider;
import com.zakgof.velvet.IVelvet;
import com.zakgof.velvet.IVelvetEnvironment;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Environments;

import java.io.File;
import java.util.ServiceLoader;
import java.util.function.Consumer;
import java.util.function.Function;

public class XodusVelvetEnv implements IVelvetEnvironment {

    private final ISerializerProvider serializerProvider;
    private final Environment env;

    public XodusVelvetEnv(File file) {
        env = Environments.newInstance(file.getAbsolutePath());

        // TODO
        serializerProvider = ServiceLoader.load(ISerializerProvider.class)
                .findFirst()
                .get();
    }


    @Override
    public IVelvet velvet() {
        return new XodusVelvet(env, null, serializerProvider.get());
    }

    @Override
    public void txnWrite(Consumer<IVelvet> action) {
        env.executeInTransaction(txn ->
            action.accept(new XodusVelvet(env, txn, serializerProvider.get()))
        );
    }

    @Override
    public <R> R txnRead(Function<IVelvet, R> action) {
        Object[] result = new Object[1];
        env.executeInReadonlyTransaction(txn -> {
            result[0] = action.apply(new XodusVelvet(env, txn, serializerProvider.get()));
        });
        return (R)result[0];
    }

    public void close() {
        env.close();
    }
}
