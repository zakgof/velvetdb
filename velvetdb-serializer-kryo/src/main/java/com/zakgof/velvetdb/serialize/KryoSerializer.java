package com.zakgof.velvetdb.serialize;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.zakgof.serialize.ISerializer;

public class KryoSerializer implements ISerializer {

    private Kryo kryo;

    public KryoSerializer() {
        this(new Kryo());
    }

    public KryoSerializer(Kryo kryo) {
        this.kryo = kryo;
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
        T object = kryo.readObject(input, clazz);
        return object;
    }

    public Kryo getKryo() {
        return kryo;
    }

}
