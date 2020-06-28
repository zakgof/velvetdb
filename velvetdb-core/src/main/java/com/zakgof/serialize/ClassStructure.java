package com.zakgof.serialize;

import com.zakgof.db.velvet.ISerializer;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClassStructure {

    private Map<String, Class<?>> fieldMap;
    private List<String> enumLabels;

    public static ClassStructure of(Class<?> clazz, ISerializer serializer) {
        return new ClassStructure(clazz, serializer);
    }

    private ClassStructure(Class<?> clazz, ISerializer serializer) {
        if (clazz.isEnum()) {
            this.enumLabels = Arrays.stream(clazz.getEnumConstants())
                .map(Enum.class::cast)
                .map(Enum::name)
                .collect(Collectors.toList());
        } else {
            List<Field> fields = serializer.getFields(clazz);
            this.fieldMap = fields.stream()
                .collect(Collectors.toMap(Field::getName, Field::getType, (u, v) -> u, LinkedHashMap::new));
        }
    }

    public Map<String, Class<?>> getFields() {
        return fieldMap;
    }

    @Override
    public int hashCode() {
        final int prime1 = 31;
        final int prime2 = 31;
        int result = 1;
        result = prime1 * result + ((fieldMap == null) ? 0 : fieldMap.hashCode());
        result = prime2 * result + ((enumLabels == null) ? 0 : enumLabels.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ClassStructure other = (ClassStructure) obj;
        if (fieldMap == null)
            return this.enumLabels.equals(other.enumLabels);
        return this.fieldMap.equals(other.fieldMap);
    }

    public String getEnumLabel(int index) {
        return enumLabels.get(index);
    }
}