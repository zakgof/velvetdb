package com.zakgof.db.velvet.island;

import com.zakgof.db.velvet.entity.IEntityDef;

public interface IIslandContext<K, V> {

    V current();

    K currentKey();

    <CK, CV> CV node(IEntityDef<CK, CV> def);

    <CK, CV> DataWrap<CK, CV> wrap(IEntityDef<CK, CV> def);

}