package com.zakgof.velvetdb.serialize;

import com.zakgof.velvet.ISerializer;
import com.zakgof.velvet.ISerializerProvider;

public class KryoSerializerProvider implements ISerializerProvider {
    @Override
    public ISerializer get() {
        return new KryoSerializer();
    }
}
