package com.zakgof.velvet.serializer;

import com.zakgof.velvet.serializer.migrator.IClassHistory;
import com.zakgof.velvet.serializer.migrator.ISerializerSchemaMigrator;

import java.util.Map;
import java.util.function.Supplier;

public interface ISerializerProviderBuilder {

    ISerializerProviderBuilder provider(String serializerProviderName);

    ISerializerProviderBuilder schemaMigrators(Map<String, ISerializerSchemaMigrator> migrators);

    ISerializerProviderBuilder schemaMigrator(String prefix, ISerializerSchemaMigrator schemaMigrator);

    ISerializerProviderBuilder classHistory(IClassHistory classHistory);

    Supplier<ISerializer> build();



}
