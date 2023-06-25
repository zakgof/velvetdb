package com.zakgof.velvet.serializer;

import com.zakgof.velvet.VelvetException;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.stream.StreamSupport;

public class SerializerFactory {
    public static ISerializerProviderBuilder builder() {
        return new SerializerProviderBuilder();
    }

    private static class SerializerProviderBuilder implements ISerializerProviderBuilder {

        private String serializerProviderName;
        private Map<String, ISerializerSchemaMigrator> migrators = new TreeMap<>();

        @Override
        public ISerializerProviderBuilder provider(String serializerProviderName) {
            this.serializerProviderName = serializerProviderName;
            return this;
        }

        @Override
        public ISerializerProviderBuilder schemaMigrators(Map<String, ISerializerSchemaMigrator> migrators) {
            this.migrators = migrators;
            return this;
        }

        @Override
        public ISerializerProviderBuilder schemaMigrator(String prefix, ISerializerSchemaMigrator schemaMigrator) {
            migrators.put(prefix, schemaMigrator);
            return this;
        }

        @Override
        public ISerializerProvider build() {
            ServiceLoader<ISerializerProvider> serializerServiceLoader = ServiceLoader.load(ISerializerProvider.class);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(serializerServiceLoader.iterator(), Spliterator.ORDERED), false)
                    .filter(sp -> serializerProviderName == null || sp.name().equals(serializerProviderName))
                    .findFirst()
                    .orElseThrow(() -> new VelvetException("No serializer implementation on classpath"));
        }
    }



}
