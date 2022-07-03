package com.zakgof.velvet;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Manage velvetdb providers.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class VelvetFactory {

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
            IVelvetProvider provider = StreamSupport.stream(Spliterators.spliteratorUnknownSize(serviceLoader.iterator(), Spliterator.ORDERED), false)
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
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(serviceLoader.iterator(), Spliterator.ORDERED), false)
                .collect(Collectors.toList());
    }

    /**
     * Generates velvetdb url for a file.
     * @param provider velvetvb provider
     * @param path file or directory path
     * @return velvetdb url
     */
    public static String urlFromPath(String provider, Path path) {
        String pathstr = path.toAbsolutePath().toUri().getPath();
        try {
            return new URI("velvetdb", provider, pathstr, null).toString();
        } catch (URISyntaxException e) {
            throw new VelvetException(e);
        }
    }
}
