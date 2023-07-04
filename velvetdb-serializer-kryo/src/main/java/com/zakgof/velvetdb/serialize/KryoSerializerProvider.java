package com.zakgof.velvetdb.serialize;

import com.zakgof.velvet.serializer.ISerializer;
import com.zakgof.velvet.serializer.ISerializerProvider;
import com.zakgof.velvet.serializer.migrator.IClassHistory;

public class KryoSerializerProvider implements ISerializerProvider {
    @Override
    public ISerializer create(IClassHistory history) {
        return new KryoSerializer(history);
    }

    @Override
    public String name() {
        return "kryo";
    }
}
