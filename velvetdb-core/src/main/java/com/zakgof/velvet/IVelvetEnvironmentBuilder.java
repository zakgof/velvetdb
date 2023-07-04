package com.zakgof.velvet;

import com.zakgof.velvet.serializer.migrator.ISerializerSchemaMigrator;

public interface IVelvetEnvironmentBuilder {
    IVelvetEnvironmentBuilder serializer(String serializerProviderName);

    IVelvetEnvironmentBuilder schemaMigrator(String prefix, ISerializerSchemaMigrator schemaMigrator);

    IVelvetEnvironment build();
}
