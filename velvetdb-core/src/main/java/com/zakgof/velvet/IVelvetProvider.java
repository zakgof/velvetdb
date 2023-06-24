package com.zakgof.velvet;

/**
 * Interface to be implemented by backed implementations.
 */
public interface IVelvetProvider {

    /**
     * Open velvetdb environment from a URI
     * @param url in format velvetdb://&lt;backendname&gt;/&lt;path&gt;
     * @return velvet environment handle
     */
    IVelvetEnvironment open(String url, ISerializerProvider serializerProvider);

    /**
     * Returns backend implementation name.
     * @return backend implementation name
     */
    String name();
}
