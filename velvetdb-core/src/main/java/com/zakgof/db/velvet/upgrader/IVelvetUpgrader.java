package com.zakgof.db.velvet.upgrader;

public interface IVelvetUpgrader {

    enum Mode {
        UPGRADE_ON_WRITE,
//      UPGRADE_ON_READ,
//      UPGRADE_ALL_NOW
    }

    IVelvetUpgrader track(Mode mode, Class<?>... classes);

    void upgrade();

}
