package com.zakgof.serialize;

import java.io.InputStream;


public interface ISerializer {
  
  public byte[] serialize(Object object);
  
  public <T> T deserialize(InputStream stream, Class<T> clazz);

}
