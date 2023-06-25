package com.zakgof.velvetdb.serialize;

import com.zakgof.velvet.serializer.ISerializer;
import com.zakgof.velvet.serializer.ISerializerProvider;

public class KryoSerializerProvider implements ISerializerProvider {
    @Override
    public ISerializer get() {
        return new KryoSerializer();
    }

    @Override
    public String name() {
        return "kryo";
    }
}
