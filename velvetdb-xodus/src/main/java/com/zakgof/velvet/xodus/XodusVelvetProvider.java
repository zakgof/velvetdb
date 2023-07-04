package com.zakgof.velvet.xodus;

import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.IVelvetProvider;
import com.zakgof.velvet.serializer.ISerializer;

import java.util.function.Supplier;

public class XodusVelvetProvider implements IVelvetProvider {

    @Override
    public IVelvetEnvironment open(String url, Supplier<ISerializer> serializerFactory) {
        return new XodusVelvetEnv(url, serializerFactory);
    }

    @Override
    public String name() {
        return "xodus";
    }

}
