package com.zakgof.velvet.serializer.migrator;

public interface IUpgrader {

    byte currentVersion(Class<? extends Object> clazz);

    ClassStructure structure(Class<?> clazz, byte classVersion);

    <T> IFixer<T> Fixer(Class<T> clazz);

}
