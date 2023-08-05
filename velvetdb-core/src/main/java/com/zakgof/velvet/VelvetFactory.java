package com.zakgof.velvet;

import com.zakgof.velvet.impl.history.VelvetClassHistory;
import com.zakgof.velvet.serializer.ISerializer;
import com.zakgof.velvet.serializer.SerializerFactory;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.NavigableSet;
import java.util.ServiceLoader;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Manage velvetdb providers.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class VelvetFactory {

    public static IVelvetEnvironment create(String providerName, String url) {
        return builder(providerName, url).build();
    }

    public static IVelvetEnvironmentBuilder builder(String providerName, String url) {
        return new VelvetEnvironmentBuilder(providerName, url);
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

    @RequiredArgsConstructor
    private static class VelvetEnvironmentBuilder implements IVelvetEnvironmentBuilder {
        private final String velvetProviderName;
        private final String velvetUrl;
        private final NavigableSet<String> watchSchema = new TreeSet<>();
        private String serializerProviderName;

        @Override
        public IVelvetEnvironmentBuilder serializer(String serializerProviderName) {
            this.serializerProviderName = serializerProviderName;
            return this;
        }

        @Override
        public IVelvetEnvironmentBuilder watchSchema(String prefix) {
            watchSchema.add(prefix);
            return this;
        }

        @Override
        public IVelvetEnvironment build() {
            ServiceLoader<IVelvetProvider> velverServiceLoader = ServiceLoader.load(IVelvetProvider.class);
            IVelvetProvider velvetProvider = StreamSupport.stream(Spliterators.spliteratorUnknownSize(velverServiceLoader.iterator(), Spliterator.ORDERED), false)
                    .filter(reg -> reg.name().equals(velvetProviderName))
                    .findFirst()
                    .orElseThrow(() -> new VelvetException("Velvetdb provider not registered: " + velvetProviderName));

            VelvetClassHistory history = new VelvetClassHistory(watchSchema);

            Supplier<ISerializer> serializerProvider = SerializerFactory.builder()
                    .provider(serializerProviderName)
                   // .schemaMigrators(migrators) TODO
                    .classHistory(history)
                    .build();

            IVelvetEnvironment velvetEnvironment = velvetProvider.open(velvetUrl, serializerProvider);
            history.setVelvet(velvetEnvironment);
            return velvetEnvironment;
        }
    }
}
