package com.zakgof.db.velvet.dynamodb;

import java.net.URI;

import com.zakgof.db.velvet.IVelvetEnvironment;
import com.zakgof.db.velvet.IVelvetProvider;

public class DynamoDBVelvetProvider implements IVelvetProvider {

    @Override
    public IVelvetEnvironment open(URI uri) {
        return new DynamoDBVelvetEnv(uri);
    }

    @Override
    public String name() {
        return "dynamodb";
    }

}
