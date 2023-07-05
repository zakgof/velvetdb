package com.zakgof.velvet.serialize;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.SerializerFactory;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.zakgof.velvet.serializer.ISerializer;
import com.zakgof.velvet.serializer.migrator.ClassStructure;
import com.zakgof.velvet.serializer.migrator.IClassHistory;
import lombok.RequiredArgsConstructor;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class KryoSerializer implements ISerializer {

    private final Kryo kryo;

    public KryoSerializer(IClassHistory classHistory) {
        this.kryo = new Kryo();
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.setRegistrationRequired(false);
        // kryo.setDefaultSerializer(new VelvetSerializerFactory(classHistory));
        kryo.setDefaultSerializer(new SerializerFactory.FieldSerializerFactory());
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
                readUsingStructure(kryo, input, type, cs);
            }
            return super.read(kryo, input, type);
        }

        private void readUsingStructure(Kryo kryo, Input input, Class<? extends T> type, ClassStructure cs) {
        }
    }

}
