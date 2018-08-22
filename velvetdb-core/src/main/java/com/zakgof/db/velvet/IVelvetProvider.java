package com.zakgof.db.velvet;

import java.net.URI;

/**
 * Interface to be implemented by backed implementations.
 */
public interface IVelvetProvider {

    /**
     * Open velvetdb environment from a URI
     * @param url in format velvetdb://&lt;backendname&gt;/&lt;path&gt;
     * @return
     */
    IVelvetEnvironment open(URI url);

    /**
     * Returns backend implementation name.
     * @return backend implementation name
     */
    String name();
}
