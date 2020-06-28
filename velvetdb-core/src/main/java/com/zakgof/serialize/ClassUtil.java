package com.zakgof.serialize;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ClassUtil {
    public static List<Field> getAllFields(Class<?> type) {
        List<Field> fields = new ArrayList<>();

        Field[] declaredFields = type.getDeclaredFields();
        Arrays.sort(declaredFields, new Comparator<Field>() {
            @Override
            public int compare(Field f1, Field f2) {
                return f1.getName().compareTo(f2.getName());
            }
        });
        for (Field field : declaredFields) {
            if ((field.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) == 0)
                fields.add(field);
        }
        if (type.getSuperclass() != null)
            fields.addAll(getAllFields(type.getSuperclass()));
        return fields;
    }
}
