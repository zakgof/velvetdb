package com.zakgof.db.velvet.datastore;

import java.net.URI;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.IVelvetProvider;

public class DatastoreVelvetProvider implements IVelvetProvider {

    @Override
    public IVelvetEnvironment open(URI uri) {
        return new DatastoreVelvetEnv(uri);
    }

    @Override
    public String name() {
        return "datastore";
    }

}
