package com.zakgof.velvet.impl.entity;

import com.zakgof.velvet.IVelvet;
import com.zakgof.velvet.entity.IEntityDef;
import com.zakgof.velvet.request.IWriteRequest;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public class SinglePutRequest<K, V> implements IWriteRequest {
    private final IEntityDef<K, V> entityDef;
    private final V value;

    @Override
    public void execute(IVelvet velvet) {
        velvet.singlePut(entityDef, value);
    }
}
