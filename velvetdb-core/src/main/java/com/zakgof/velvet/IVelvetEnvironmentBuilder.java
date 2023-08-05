package com.zakgof.velvet;

public interface IVelvetEnvironmentBuilder {
    IVelvetEnvironmentBuilder serializer(String serializerProviderName);

    IVelvetEnvironmentBuilder watchSchema(String prefix);

    IVelvetEnvironment build();
}
