package com.zakgof.db.velvet.xodus;

import java.io.File;
import java.net.URI;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.IVelvetProvider;

public class XodusVelvetProvider implements IVelvetProvider {

    @Override
    public IVelvetEnvironment open(URI uri) {
        return new XodusVelvetEnv(new File(uri.getPath()));
    }

    @Override
    public String name() {
        return "xodus";
    }

}
