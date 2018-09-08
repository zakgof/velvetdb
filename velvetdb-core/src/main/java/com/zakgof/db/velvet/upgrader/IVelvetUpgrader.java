package com.zakgof.db.velvet.upgrader;

import com.zakgof.serialize.IFixer;

public interface IVelvetUpgrader {

    enum Mode {
        UPGRADE_ON_WRITE,
//      UPGRADE_ON_READ,
//      UPGRADE_ALL_NOW
    }

    IVelvetUpgrader trackClasses(Mode mode, Class<?>... classes);

    IVelvetUpgrader trackPackages(Mode mode, String... packages);

    <T> IVelvetUpgrader fix(Class<T> clazz, IFixer<T> fixer);
}
