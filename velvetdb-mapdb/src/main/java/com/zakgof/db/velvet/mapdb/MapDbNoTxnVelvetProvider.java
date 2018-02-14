package com.zakgof.db.velvet.mapdb;

import java.io.File;
import java.net.URI;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.IVelvetProvider;

public class MapDbNoTxnVelvetProvider implements IVelvetProvider {

    @Override
    public IVelvetEnvironment open(URI uri) {
        return new MapDbNoTxnEnv(new File(uri));
    }

    @Override
    public String name() {
        return "mapdb-notxn";
    }

}
