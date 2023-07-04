package com.zakgof.velvet.impl.history;

import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.VelvetException;
import com.zakgof.velvet.annotation.Key;
import com.zakgof.velvet.annotation.SortedKey;
import com.zakgof.velvet.entity.Entities;
import com.zakgof.velvet.entity.ISortedEntityDef;
import com.zakgof.velvet.serializer.migrator.ClassStructure;
import com.zakgof.velvet.serializer.migrator.EnumStructure;
import com.zakgof.velvet.serializer.migrator.IClassHistory;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

public class VelvetClassHistory implements IClassHistory {

    private IVelvetEnvironment velvetEnvironment;
    private final Map<Class<?>, Integer> currentVersionCache = new HashMap<>();

    public void setVelvet(IVelvetEnvironment velvetEnvironment) {
        this.velvetEnvironment = velvetEnvironment;
    }

    @Override
    public int currentVersion(Class<?> clazz) {
        return currentVersionCache.computeIfAbsent(clazz, this::loadCurrentClassVersion);
    }

    private int loadCurrentClassVersion(Class<?> clazz) {
        ISortedEntityDef<Integer, ClassStructureRecord> classRec = entityFor(clazz);

        ClassStructureRecord csr = classRec.index()
                .query()
                .last()
                .execute(velvetEnvironment);

        int highestVersion = csr != null ? csr.version : -1;

        ClassStructure cs = ClassStructure.of(clazz);
        while(csr != null) {
            if (csr.structure.equals(cs)) {
                return csr.version;
            }

            csr = classRec.index()
                    .query()
                    .prev(csr)
                    .execute(velvetEnvironment);
        }
        // Not found, register a new version
        int newVersion = highestVersion + 1;
        csr = new ClassStructureRecord(newVersion, cs);
        classRec.put()
                .value(csr)
                .execute(velvetEnvironment);

        currentVersionCache.put(clazz, newVersion);

        return newVersion;
    }

    private ISortedEntityDef<Integer, ClassStructureRecord> entityFor(Class<?> clazz) {
        return Entities.from(ClassStructureRecord.class)
                .kind("velvet.class.history:" + clazz.getName())
                .makeSorted();
    }

    @Override
    public ClassStructure classStructure(Class<?> clazz, int classVersion) {
        ClassStructureRecord csr = entityFor(clazz).get()
                .key(classVersion)
                .execute(velvetEnvironment);
        if (csr == null) {
            throw new VelvetException("Cannot find class " + clazz + " version " + classVersion + " in the history");
        }
        return csr.structure;
    }

    @Override
    public EnumStructure enumStructure(Class<?> clazz, int classVersion) {
        return null;
    }

    @RequiredArgsConstructor
    private static class ClassStructureRecord {
        @SortedKey
        private final int version;
        private final ClassStructure structure;
    }

    @RequiredArgsConstructor
    private static class EnumStructureRecord {
        @SortedKey
        private final int version;
        private final EnumStructure structure;
    }
}
