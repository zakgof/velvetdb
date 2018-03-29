package com.zakgof.db.velvet;

import java.net.URI;

public interface IVelvetProvider {

    IVelvetEnvironment open(URI u);

    String name();
}
