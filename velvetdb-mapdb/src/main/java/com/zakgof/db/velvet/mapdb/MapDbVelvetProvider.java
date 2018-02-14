package com.zakgof.db.velvet.mapdb;

import java.io.File;
import java.net.URI;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.IVelvetProvider;

public class MapDbVelvetProvider implements IVelvetProvider {

    @Override
    public IVelvetEnvironment open(URI path) {
        return new MapDbVelvetEnv(new File(path));
    }

    @Override
    public String name() {
        return "mapdb";
    }

}
