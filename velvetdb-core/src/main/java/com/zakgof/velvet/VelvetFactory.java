package com.zakgof.velvet;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

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

    public static IVelvetEnvironment open(String providerName, String url) {
        ServiceLoader<IVelvetProvider> serviceLoader = ServiceLoader.load(IVelvetProvider.class);
        IVelvetProvider provider = StreamSupport.stream(Spliterators.spliteratorUnknownSize(serviceLoader.iterator(), Spliterator.ORDERED), false)
                .filter(reg -> reg.name().equals(providerName))
                .findFirst()
                .orElseThrow(() -> new VelvetException("Velvetdb backend not registered: " + providerName));

        // TODO: support multiple serializers
        ISerializerProvider serializerProvider = ServiceLoader.load(ISerializerProvider.class)
                .findFirst()
                .orElseThrow(() ->  new VelvetException("No serializer implementation on classpath"));
        return provider.open(url, serializerProvider);
    }

    /**
     * Enumerates registered velvetdb providers (backends).
     *
     * @return providers list
     */
    public static List<IVelvetProvider> getProviders() {
        ServiceLoader<IVelvetProvider> serviceLoader = ServiceLoader.load(IVelvetProvider.class);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(serviceLoader.iterator(), Spliterator.ORDERED), false)
                .collect(Collectors.toList());
    }
}
