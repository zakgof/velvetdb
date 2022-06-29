package com.zakgof.velvet.lmdb;

import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.IVelvetProvider;

import java.io.File;
import java.net.URI;

public class LmdbVelvetProvider implements IVelvetProvider {

    @Override
    public IVelvetEnvironment open(URI uri) {
        return new LmdbVelvetEnv(new File(uri.getPath()));
    }

    @Override
    public String name() {
        return "lmdb";
    }

}
