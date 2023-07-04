package com.zakgof.velvet;

import com.zakgof.velvet.serializer.ISerializer;

import java.util.function.Supplier;

/**
 * Interface to be implemented by backed implementations.
 */
public interface IVelvetProvider {

    /**
     * Open velvetdb environment from a URI
     * @param url in format velvetdb://&lt;backendname&gt;/&lt;path&gt;
     * @param  serializerFactory serializer supplier
     * @return velvet environment handle
     */
    IVelvetEnvironment open(String url, Supplier<ISerializer> serializerFactory);

    /**
     * Returns backend implementation name.
     * @return backend implementation name
     */
    String name();
}
