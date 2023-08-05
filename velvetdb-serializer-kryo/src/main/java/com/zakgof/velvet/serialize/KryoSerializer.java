package com.zakgof.velvet.serialize;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.MapperField;
import com.zakgof.velvet.VelvetException;
import com.zakgof.velvet.serializer.ISerializer;
import com.zakgof.velvet.serializer.migrator.ClassStructure;
import com.zakgof.velvet.serializer.migrator.IClassHistory;
import lombok.RequiredArgsConstructor;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.stream.Collectors;

public class KryoSerializer implements ISerializer {

    private final Kryo kryo;

    public KryoSerializer(IClassHistory classHistory) {
        this.kryo = new Kryo();
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.setRegistrationRequired(false);
        kryo.setDefaultSerializer(new VelvetSerializerFactory(classHistory));
        // kryo.setDefaultSerializer(new SerializerFactory.FieldSerializerFactory());
    }

    @Override
    public <T> byte[] serialize(T object, Class<T> clazz) {
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        Output output = new Output(bas);
        kryo.writeObject(output, object);
        output.flush();
        return bas.toByteArray();
    }

    @Override
    public <T> T deserialize(InputStream stream, Class<T> clazz) {
        Input input = new Input(stream);
        return kryo.readObject(input, clazz);
    }

    public Kryo getKryo() {
        return kryo;
    }

    @RequiredArgsConstructor
    private static class VelvetSerializerFactory implements com.esotericsoftware.kryo.SerializerFactory<VelvetSerializer> {

        private final IClassHistory classHistory;

        @Override
        public VelvetSerializer newSerializer(Kryo kryo, Class type) {
            return new VelvetSerializer(kryo, type, classHistory);
        }

        @Override
        public boolean isSupported(Class type) {
            return true;
        }
    }

    private static class VelvetSerializer<T> extends FieldSerializer<T> {

        private final IClassHistory classHistory;

        public VelvetSerializer(Kryo kryo, Class type, IClassHistory classHistory) {
            super(kryo, type);
            this.classHistory = classHistory;
        }

        @Override
        public void write(Kryo kryo, Output output, T object) {
            int version = classHistory.currentVersion(object.getClass());
            output.writeVarInt(version, true);
            super.write(kryo, output, object);
        }

        @Override
        public T read(Kryo kryo, Input input, Class<? extends T> type) {
            int actualVersion = classHistory.currentVersion(type);
            int storedVersion = input.readVarInt(true);
            if (actualVersion != storedVersion) {
                ClassStructure cs = classHistory.classStructure(type, storedVersion);
                return readUsingStructure(kryo, input, type, cs);
            }
            return super.read(kryo, input, type);
        }

        private T readUsingStructure(Kryo kryo, Input input, Class<? extends T> type, ClassStructure cs) {
            T object = create(kryo, input, type);
            kryo.reference(object);
            Map<String, Object> objectMap = readAsMap(input, cs);

            objectMap.forEach((fieldName, fieldValue) -> {
                try {
                    CachedField cachedField = getField(fieldName);
                    cachedField.getField().set(object, fieldValue);
                } catch (IllegalArgumentException e) {
                    // Field removed - ignore
                } catch (ReflectiveOperationException e) {
                    throw new VelvetException(e);
                }
            });
            return object;
        }

        private Map<String, Object> readAsMap(Input input, ClassStructure cs) {
            return cs.getFields()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> readField(input, entry.getValue())));
        }

        private Object readField(Input input, Class<?> clazz) {
            if (clazz == boolean.class) {
                return input.readBoolean();
            } else if (clazz == byte.class) {
                return input.readByte();
            } else if (clazz == short.class) {
                return input.readShort();
            } else if (clazz == int.class) {
                return input.readVarInt(false);
            } else if (clazz == char.class) {
                return input.readChar();
            } else if (clazz == long.class) {
                return input.readLong();
            } else {
                MapperField mf = new MapperField(this, clazz);
                mf.read(input, null);
                return mf.fetch();
            }
        }
    }
}
