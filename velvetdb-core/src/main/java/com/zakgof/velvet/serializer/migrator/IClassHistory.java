package com.zakgof.velvet.serializer.migrator;

public interface IClassHistory {

    int currentVersion(Class<? extends Object> clazz);

    ClassStructure classStructure(Class<?> clazz, int classVersion);

    EnumStructure enumStructure(Class<?> clazz, int classVersion);

}
