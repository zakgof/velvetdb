package com.zakgof.db.velvet.impl;

import java.util.function.Supplier;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.VelvetException;
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
            if (serializer instanceof ZeSerializer) {
                ((ZeSerializer)serializer).setUpgrader(upgrader);
            } else {
                throw new VelvetException("Upgrader can only be used with ZeSerializer");
            }
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
}
