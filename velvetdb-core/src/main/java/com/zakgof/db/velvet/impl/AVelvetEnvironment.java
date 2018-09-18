package com.zakgof.db.velvet.impl;

import java.util.function.Supplier;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.upgrader.IVelvetUpgrader;
import com.zakgof.serialize.ISerializer;
import com.zakgof.serialize.ZeSerializer;

public abstract class AVelvetEnvironment implements IVelvetEnvironment {

    private VelvetUpgraderImpl upgrader;
    private Supplier<ISerializer> serializerSupplier = () -> new ZeSerializer();

    @Override
    public void setSerializer(Supplier<ISerializer> serializer) {
        this.serializerSupplier = serializer;
    }

    protected ISerializer instantiateSerializer() {
        ISerializer serializer = serializerSupplier.get();
        if (upgrader != null) {
            serializer.setUpgrader(upgrader);
        }
        return serializer;
    }

    @Override
    public IVelvetUpgrader upgrader() {
        if (upgrader == null) {
            upgrader = new VelvetUpgraderImpl(this);
        }
        return upgrader;
    }

    public ISerializer getSerializer() {
        return serializerSupplier.get();
    }
}
