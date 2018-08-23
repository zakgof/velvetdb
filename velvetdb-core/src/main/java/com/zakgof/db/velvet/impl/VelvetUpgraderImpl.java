package com.zakgof.db.velvet.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.annotation.Key;
import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.query.SecQueries;
import com.zakgof.db.velvet.upgrader.IVelvetUpgrader;
import com.zakgof.serialize.ClassStructure;
import com.zakgof.serialize.IUpgrader;

class VelvetUpgraderImpl implements IVelvetUpgrader, IUpgrader {

    private static class ClassVersion {

        public ClassVersion(String className, byte version, ClassStructure structure) {
            this.className = className;
            this.version = version;
            this.structure = structure;
        }

        private String className;
        private byte version;
        private ClassStructure structure;

        @Key
        public String getKey() {
            return className + "-" + version;
        }

        public String getClassName() {
            return className;
        }

        public byte getVersion() {
            return version;
        }
    }

    private final AVelvetEnvironment env;

    private Map<Class<?>, Byte> actualVersions = new HashMap<>();

    private IEntityDef<String, ClassVersion> CLASS_VERSION = Entities.from(ClassVersion.class)
        .index("class", ClassVersion::getClassName, String.class)
        .index("version", ClassVersion::getVersion, byte.class)
        .make();

    private Map<Class<?>, Mode> classMap = new HashMap<>();
    private Cache<String, ClassStructure> classStructCache = CacheBuilder.newBuilder().maximumSize(2048).build();

    VelvetUpgraderImpl(AVelvetEnvironment aVelvetEnvironment) {
        env = aVelvetEnvironment;
    }

    @Override
    public IVelvetUpgrader track(Mode mode, Class<?>... classes) {
        // TODO: implement modes
        for (Class<?> clazz : classes) {
            classMap.put(clazz, mode);
        }
        return this;
    }

    @Override
    public void upgrade() {
        for (Class<?> clazz : classMap.keySet()) {
            upgrade(clazz);
        }
    }

    private void upgrade(Class<?> clazz) {
        ClassStructure actualStructure = ClassStructure.of(clazz);

       // TODO: PERF: only load biggest version number, then climb down
       List<ClassVersion> versions = env.calculate(velvet -> CLASS_VERSION.queryList(velvet, "class", SecQueries.eq(clazz.getName())));
       for (ClassVersion version : versions) {
            if (version.structure.equals(actualStructure)) {
                actualVersions.put(clazz, version.version);
                return;
            }
       }
       // Version not found
       int lastKnownVersion = versions.stream().mapToInt(ClassVersion::getVersion).max().orElse(-1);
       byte currentVersion = (byte)(lastKnownVersion + 1);
       actualVersions.put(clazz, currentVersion);
       ClassVersion cv = new ClassVersion(clazz.getName(), currentVersion, actualStructure);
       env.execute(velvet -> CLASS_VERSION.put(velvet, cv));
    }

    @Override
    public byte getCurrentVersionOf(Class<?> clazz) {
        return actualVersions.getOrDefault(clazz, (byte)0);
    }

    @Override
    public ClassStructure getStructureFor(Class<?> clazz, byte classVersion) {
        String key = clazz.getName() + "-" + classVersion;
        try {
            ClassStructure cs = classStructCache.get(key, () -> {
                ClassVersion version = env.calculate(velvet -> CLASS_VERSION.get(velvet, key));
                return version == null ? null : version.structure;
            });
            return cs;
        } catch (ExecutionException e) {
            throw new VelvetException(e);
        }
    }

}