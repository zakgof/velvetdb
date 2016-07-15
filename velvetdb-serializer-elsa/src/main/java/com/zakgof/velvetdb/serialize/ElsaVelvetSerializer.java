package com.zakgof.velvetdb.serialize;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;

import com.zakgof.serialize.ISerializer;

public class ElsaVelvetSerializer implements ISerializer {

  private ElsaSerializer elsa;

  public ElsaVelvetSerializer() {
    this(new ElsaMaker().make())
  }

  public ElsaVelvetSerializer(ElsaSerializer elsa) {
    this.elsa = elsa;
  }

  public byte[] serialize(Object object) {
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bas);
    elsa.serialize(dos, object);
    return bas.toByteArray();
  }

  public <T> T deserialize(InputStream stream, Class<T> clazz) {
    DataInputStream in = new DataInputStream(stream);
    T object = elsa.deserialize(in);
    return object;
  }

  public ElsaSerializer getElsa() {
    return elsa;
  }

}
