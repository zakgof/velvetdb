package com.zakgof.db.velvet;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.impl.cache.CachingVelvetEnvironment;
import com.zakgof.tools.generic.Functions;

/**
 * Manage velvetdb providers.
 */
public class VelvetFactory {

    /**
     * Connects to a velvetdb database with RAM caching enabled. Cached velvetdb is faster, but it can only be used if no other client is concurrently working qith the same database.
     * @param url velvetdb url in format velvetdb://&lt;backendname&gt;/&lt;path&gt;
     * @return velvet env
     */
    public static IVelvetEnvironment openCaching(String url) {
        return new CachingVelvetEnvironment(open(url));
    }

    /**
     * Connects to a velvetdb database.
     * @param url velvetdb url in format velvetdb://&lt;backendname&gt;/&lt;path&gt;
     * @return velvet env
     */
    public static IVelvetEnvironment open(String url) {
        try {
            URI u = new URI(url);
            if (!u.getScheme().equals("velvetdb"))
                throw new VelvetException("Url protocol should be velvetdb://");
            String name = u.getHost();

            ServiceLoader<IVelvetProvider> serviceLoader = ServiceLoader.load(IVelvetProvider.class);
            IVelvetProvider provider = Functions.stream(serviceLoader.iterator())
              .filter(reg -> reg.name().equals(name))
              .findFirst()
              .orElseThrow(() -> new VelvetException("Velvetdb backend not registered: " + name));
            return provider.open(u);
        } catch (URISyntaxException e) {
            throw new VelvetException(e);
        }
    }

    /**
     * Enumerates registered velvetdb providers (backends).
     * @return providers list
     */
    public static List<IVelvetProvider> getProviders() {
        ServiceLoader<IVelvetProvider> serviceLoader = ServiceLoader.load(IVelvetProvider.class);
        return Functions.stream(serviceLoader.iterator()).collect(Collectors.toList());
    }
}
