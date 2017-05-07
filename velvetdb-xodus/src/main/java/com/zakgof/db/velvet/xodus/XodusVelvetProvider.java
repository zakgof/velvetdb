package com.zakgof.db.velvet.xodus;

import java.io.File;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.IVelvetProvider;

public class XodusVelvetProvider implements IVelvetProvider {

    @Override
    public IVelvetEnvironment open(String path) {
        return new XodusVelvetEnv(new File(path));
    }

    @Override
    public String name() {
        return "xodus";
    }

}
