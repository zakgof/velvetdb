package com.zakgof.velvet.serializer;

import com.zakgof.velvet.serializer.migrator.IClassHistory;

public interface ISerializerProvider {

    String name();

    ISerializer create(IClassHistory history);

}
