package com.zakgof.db.velvet.impl.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.cache.Cache;
import com.zakgof.db.txn.ITransactionCall;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.upgrader.IVelvetUpgrader;
import com.zakgof.serialize.ISerializer;

public class CachingVelvetEnvironment implements IVelvetEnvironment {

    private final IVelvetEnvironment proxyEnv;
    private Map<String, Cache<?, ?>> cacheContainer = new HashMap<>();

    public CachingVelvetEnvironment(IVelvetEnvironment proxyEnv) {
        this.proxyEnv = proxyEnv;
    }

    @Override
    public void execute(ITransactionCall<IVelvet> transaction) {
        proxyEnv.execute(proxyVelvet -> {
            CachingVelvet velvet = new CachingVelvet(proxyVelvet, cacheContainer);
            transaction.execute(velvet);
        });
    }

    @Override
    public void close() {
        proxyEnv.close();
        cacheContainer.clear();
    }

    @Override
    public void setSerializer(Supplier<ISerializer> serializer) {
        proxyEnv.setSerializer(serializer);
    }

    @Override
    public IVelvetUpgrader upgrader() {
        return proxyEnv.upgrader();
    }

}
