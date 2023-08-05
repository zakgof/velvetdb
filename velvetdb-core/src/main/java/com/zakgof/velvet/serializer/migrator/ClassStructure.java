package com.zakgof.velvet.serializer.migrator;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
@RequiredArgsConstructor
public class ClassStructure {

    private final Map<String, Class<?>> fields;

    private static Map<String, Class<?>> collectFields(Class<?> clazz) {
        Map<String, Class<?>> fieldMap = new LinkedHashMap<>();
        Field[] declaredFields = clazz.getDeclaredFields();
        Arrays.sort(declaredFields, Comparator.comparing(Field::getName));
        for (Field field : declaredFields) {
            if ((field.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) == 0) {
                fieldMap.put(field.getName(), field.getType());
            }
        }
        if (clazz.getSuperclass() != null)
            fieldMap.putAll(collectFields(clazz.getSuperclass()));
        return fieldMap;
    }

    public static ClassStructure of(Class<?> clazz) {
       return new ClassStructure(collectFields(clazz));
    }
}
