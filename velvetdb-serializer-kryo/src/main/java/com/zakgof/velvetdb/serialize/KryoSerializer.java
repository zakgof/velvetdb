package com.zakgof.velvetdb.serialize;

import static com.esotericsoftware.kryo.Kryo.NULL;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.zakgof.serialize.ClassStructure;
import com.zakgof.serialize.IFixer;
import com.zakgof.serialize.ISerializer;
import com.zakgof.serialize.IUpgrader;
import com.zakgof.serialize.ZeSerializer;
import com.zakgof.serialize.ZeSerializerException;

public class KryoSerializer implements ISerializer {

    private Kryo kryo;
    private IUpgrader upgrader;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public KryoSerializer() {
        this.kryo = new Kryo();
        kryo.addDefaultSerializer(Enum.class, (kr, type) -> new UpgradableEnumSerializer(kr, type));
        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        kryo.setDefaultSerializer((kr, type) -> new UpgradableFieldSerializer<>(kr, type));
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

    @Override
    public void setUpgrader(IUpgrader upgrader) {
        this.upgrader = upgrader;
    }

    private class UpgradableFieldSerializer<T> extends FieldSerializer<T> {

        public UpgradableFieldSerializer(Kryo kryo, Class<T> clazz) {
            super(kryo, clazz);
        }

        @Override
        public T read(Kryo kryo, Input input, Class<T> type) {
            int version = input.readVarInt(true);
            if (upgrader != null) {
                byte currentVersion = upgrader.getCurrentVersionOf(type);
                if (version != currentVersion) {
                    ClassStructure cs = upgrader.getStructureFor(type, (byte)version);
                    if (cs == null) {
                        throw new ZeSerializerException("Upgrader cannot provide " + type.getName() + " ver." + version);
                    }

                    Map<String, Object> oldFieldValues = new HashMap<>();

                    T object = create(kryo, input, type);
                    kryo.reference(object);

                    for (Entry<String, Class<?>> entry : cs.getFields().entrySet()) {
                        String fieldName = entry.getKey();
                        Class<?> storedFieldType = entry.getValue();
                        Object fieldValue = storedFieldType.isPrimitive() ? kryo.readObject(input, storedFieldType) : kryo.readObjectOrNull(input, storedFieldType);
                        oldFieldValues.put(fieldName, fieldValue);
                        Field field = ZeSerializer.findSerializableField(type, fieldName, storedFieldType);
                        if (field != null) {
                            field.setAccessible(true);
                            try {
                                field.set(object, fieldValue);
                            } catch (IllegalArgumentException | IllegalAccessException e) {
                                throw new ZeSerializerException(e);
                            }
                        }
                    }
                    IFixer<T> fixer = upgrader.getFixerFor(type);
                    if (fixer != null) {
                        return fixer.fix(object, oldFieldValues);
                    }
                    return object;
                }
            }
            return super.read(kryo, input, type);
        }

        @Override
        public void write(Kryo kryo, Output output, T object) {
            int version = upgrader == null ? 0 : upgrader.getCurrentVersionOf(object.getClass());
            output.writeVarInt(version, true);
            super.write(kryo, output, object);
        }

    }

    @Override
    public List<Field> getFields(Class<?> clazz) {
        try {
            List<Field> allFields = new ArrayList<>();
            Class<?> nextClass = clazz;
            while (nextClass != Object.class) {
                Field[] declaredFields = nextClass.getDeclaredFields();
                if (declaredFields != null) {
                    for (Field f : declaredFields) {
                        if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers()))
                            continue;
                        allFields.add(f);
                    }
                }
                nextClass = nextClass.getSuperclass();
            }
            allFields.sort(Comparator.comparing(Field::getName));
            return allFields;

        } catch (Exception e) {
        }
        return ZeSerializer.getAllFields(clazz);
    }

    private class UpgradableEnumSerializer<T extends Enum<T>> extends Serializer<T> {

        private Class<T> enumType;

        public UpgradableEnumSerializer(Kryo kryo, Class<T> enumType) {
            this.enumType = enumType;
        }

        @Override
        public void write(Kryo kryo, Output output, @SuppressWarnings("rawtypes") Enum object) {
            if (object == null) {
                output.writeVarInt(NULL, true);
            } else {
                int version = upgrader == null ? 0 : upgrader.getCurrentVersionOf(object.getClass());
                output.writeVarInt(1 + version, true);
                output.writeVarInt(object.ordinal() + 1, true);
            }
        }

        @SuppressWarnings("rawtypes")
        @Override
        public T read(Kryo kryo, Input input, Class<T> type) {
            int version = input.readVarInt(true);
            if (version == NULL) {
                return null;
            }
            version--;

            if (upgrader != null) {
                byte currentVersion = upgrader.getCurrentVersionOf(type);
                if (version != currentVersion) {
                    ClassStructure cs = upgrader.getStructureFor(type, (byte)version);
                    if (cs == null) {
                        throw new ZeSerializerException("Upgrader cannot provide " + type.getName() + " ver." + version);
                    }
                    String name = cs.getEnumLabel(input.readVarInt(true) - 1);
                    return Enum.valueOf(enumType, name);
                }
            }

            int ordinal = input.readVarInt(true);
            if (ordinal == NULL) {
                return null;
            }
            ordinal--;
            Enum[] enumConstants = type.getEnumConstants();
            if (ordinal < 0 || ordinal > enumConstants.length - 1)
                throw new KryoException("Invalid ordinal for enum \"" + type.getName() + "\": " + ordinal);
            Object constant = enumConstants[ordinal];
            return type.cast(constant);
        }

    }
}
