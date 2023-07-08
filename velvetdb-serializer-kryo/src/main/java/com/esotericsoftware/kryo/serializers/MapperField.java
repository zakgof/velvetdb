package com.esotericsoftware.kryo.serializers;

import com.esotericsoftware.kryo.util.Generics;

public class MapperField extends ReflectField {

    private Object value;

    public <T> MapperField(FieldSerializer<T> serializer, Class<?> clazz) {
        super(null, serializer, new Generics.GenericType(clazz, clazz, clazz));
    }

    @Override
    public void set(Object object, Object value) throws IllegalAccessException {
        this.value = value;
    }

    public Object fetch() {
        return value;
    }
}
