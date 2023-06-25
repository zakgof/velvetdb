package com.zakgof.velvet.serializer;

import java.util.Map;

public interface ISerializerProviderBuilder {
    ISerializerProviderBuilder provider(String serializerProviderName);

    ISerializerProviderBuilder schemaMigrators(Map<String, ISerializerSchemaMigrator> migrators);

    ISerializerProviderBuilder schemaMigrator(String prefix, ISerializerSchemaMigrator schemaMigrator);

    ISerializerProvider build();
}
