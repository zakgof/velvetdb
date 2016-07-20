package com.zakgof.db.velvet.mapdb;

import java.io.File;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.IVelvetProvider;

public class MapDbVelvetProvider implements IVelvetProvider {

    @Override
    public IVelvetEnvironment open(String path) {
        return new MapDbNoTxnEnv(new File(path));
    }

    @Override
    public String name() {
        return "mapdb-notxn";
    }

}