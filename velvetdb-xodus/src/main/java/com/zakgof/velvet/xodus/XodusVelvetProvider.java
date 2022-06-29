package com.zakgof.velvet.xodus;

import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.IVelvetProvider;

import java.io.File;
import java.net.URI;

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
