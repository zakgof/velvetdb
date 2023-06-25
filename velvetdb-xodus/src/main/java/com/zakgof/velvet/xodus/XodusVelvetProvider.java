package com.zakgof.velvet.xodus;

import com.zakgof.velvet.serializer.ISerializerProvider;
import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.IVelvetProvider;

public class XodusVelvetProvider implements IVelvetProvider {

    @Override
    public IVelvetEnvironment open(String url, ISerializerProvider serializerProvider) {
        return new XodusVelvetEnv(url, serializerProvider);
    }

    @Override
    public String name() {
        return "xodus";
    }

}
