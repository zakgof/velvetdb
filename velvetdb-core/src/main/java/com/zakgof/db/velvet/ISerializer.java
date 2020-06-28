package com.zakgof.db.velvet;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.List;

public interface ISerializer {

    public <T> byte[] serialize(T object, Class<T> clazz);

    public <T> T deserialize(InputStream stream, Class<T> clazz);

    public void setUpgrader(IUpgrader upgrader);

    public List<Field> getFields(Class<?> clazz);

}
