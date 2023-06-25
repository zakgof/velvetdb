package com.zakgof.velvetdb.serialize;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.SerializerFactory;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.zakgof.velvet.serializer.ISerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class KryoSerializer implements ISerializer {

    private final Kryo kryo;

    public KryoSerializer() {
        this.kryo = new Kryo();
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.setRegistrationRequired(false);
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
}
