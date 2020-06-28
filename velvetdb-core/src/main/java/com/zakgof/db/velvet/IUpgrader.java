package com.zakgof.db.velvet;

import com.zakgof.serialize.ClassStructure;

public interface IUpgrader {

    byte getCurrentVersionOf(Class<? extends Object> clazz);

    ClassStructure getStructureFor(Class<?> clazz, byte classVersion);

    <T> IFixer<T> getFixerFor(Class<T> clazz);

}
