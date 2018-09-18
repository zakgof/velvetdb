package com.zakgof.velvetdb.serialize;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.mapdb.elsa.ElsaMaker;
import org.mapdb.elsa.ElsaSerializer;

import com.zakgof.db.velvet.VelvetException;
import com.zakgof.serialize.ISerializer;

public class ElsaVelvetSerializer implements ISerializer {

    private ElsaSerializer elsa;

    public ElsaVelvetSerializer() {
        this(new ElsaMaker().make());
    }

    public ElsaVelvetSerializer(ElsaSerializer elsa) {
        this.elsa = elsa;
    }

    @Override
    public <T> byte[] serialize(T object, Class<T> clazz) {
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bas);
        try {
            elsa.serialize(dos, object);
        } catch (IOException e) {
            throw new VelvetException(e);
        }
        return bas.toByteArray();
    }

    @Override
    public <T> T deserialize(InputStream stream, Class<T> clazz) {
        DataInputStream in = new DataInputStream(stream);
        T object;
        try {
            object = elsa.deserialize(in);
        } catch (IOException e) {
            throw new VelvetException(e);
        }
        return object;
    }

    public ElsaSerializer getElsa() {
        return elsa;
    }

}
