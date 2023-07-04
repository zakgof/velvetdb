package com.zakgof.velvet.serializer;

import java.io.InputStream;

public interface ISerializer {

    public <T> byte[] serialize(T object, Class<T> clazz);

    public <T> T deserialize(InputStream stream, Class<T> clazz);

}
