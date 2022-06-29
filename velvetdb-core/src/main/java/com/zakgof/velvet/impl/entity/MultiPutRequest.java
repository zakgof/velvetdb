package com.zakgof.velvet.impl.entity;

import com.zakgof.velvet.IVelvet;
import com.zakgof.velvet.entity.IEntityDef;
import com.zakgof.velvet.request.IWriteRequest;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Collection;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public class MultiPutRequest<K, V> implements IWriteRequest {

    private final IEntityDef<K, V> entityDef;
    private final Collection<V> values;

    @Override
    public void execute(IVelvet velvet) {
        velvet.multiPut(entityDef, values);
    }
}
