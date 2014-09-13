package com.zakgof.serialize;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.zakgof.tools.io.ISimpleSerializer;
import com.zakgof.tools.io.SimpleBooleanSerializer;
import com.zakgof.tools.io.SimpleByteArraySerializer;
import com.zakgof.tools.io.SimpleDateSerializer;
import com.zakgof.tools.io.SimpleDoubleSerializer;
import com.zakgof.tools.io.SimpleFloatSerializer;
import com.zakgof.tools.io.SimpleInputStream;
import com.zakgof.tools.io.SimpleIntegerSerializer;
import com.zakgof.tools.io.SimpleLongSerializer;
import com.zakgof.tools.io.SimpleOutputStream;
import com.zakgof.tools.io.SimpleStringSerializer;

@SuppressWarnings("rawtypes")
public class ZeSerializer implements ISerializer {

  @Override
  public byte[] serialize(Object object) {
    // long start = System.currentTimeMillis();
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
      SimpleOutputStream sos = new SimpleOutputStream(baos);
      fieldSerializer.write(object, object.getClass(), sos);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      // long end = System.currentTimeMillis();
      // Log.w("kotik", "serialize " + object.getClass().getSimpleName() + " in " + (end-start));
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T deserialize(InputStream is, Class<T> clazz) {
    // long start = System.currentTimeMillis();
    try {
      SimpleInputStream sis = new SimpleInputStream(is);
      return (T) fieldSerializer.read(sis, clazz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      // long end = System.currentTimeMillis();
      // Log.w("kotik", "deserialize " + clazz.getSimpleName() + " in " + (end-start));
    }
  }

  public static List<Field> getAllFields(Class<?> type) {
    List<Field> fields = new ArrayList<Field>();
    
    Field[] declaredFields = type.getDeclaredFields();
    Arrays.sort(declaredFields, new Comparator<Field>() {
      @Override
      public int compare(Field f1, Field f2) {
        return f1.getName().compareTo(f2.getName());
      }
    });
    for (Field field : declaredFields)
      fields.add(field);
    if (type.getSuperclass() != null)
      fields.addAll(getAllFields(type.getSuperclass()));
    return fields;
  }

  private interface IFieldSerializer<T> {
    void write(T object, Class<? extends T> clazz, SimpleOutputStream sos) throws IOException;

    T read(SimpleInputStream sis, Class<? extends T> clazz) throws IOException;
  }

  private static final IFieldSerializer<Object> fieldSerializer = new FieldSerializer();

  private static final Map<Class<?>, ISimpleSerializer<?>> serializers = new HashMap<Class<?>, ISimpleSerializer<?>>();

  static {
    serializers.put(int.class, SimpleIntegerSerializer.INSTANCE);
    serializers.put(long.class, SimpleLongSerializer.INSTANCE);
    serializers.put(float.class, SimpleFloatSerializer.INSTANCE);
    serializers.put(double.class, SimpleDoubleSerializer.INSTANCE);
    serializers.put(boolean.class, SimpleBooleanSerializer.INSTANCE);
    serializers.put(Integer.class, SimpleIntegerSerializer.INSTANCE);
    serializers.put(Long.class, SimpleLongSerializer.INSTANCE);
    serializers.put(Float.class, SimpleFloatSerializer.INSTANCE);
    serializers.put(Double.class, SimpleDoubleSerializer.INSTANCE);
    serializers.put(Boolean.class, SimpleBooleanSerializer.INSTANCE);
    serializers.put(String.class, SimpleStringSerializer.INSTANCE);
    serializers.put(Date.class, SimpleDateSerializer.INSTANCE);
    serializers.put(byte[].class, SimpleByteArraySerializer.INSTANCE);
    serializers.put(HashMap.class, new HashMapSerializer());
    serializers.put(ArrayList.class, new CollectionSerializer<ArrayList>(ArrayList.class));
    serializers.put(Map.class, new HashMapSerializer());
    serializers.put(List.class, new CollectionSerializer<ArrayList>(ArrayList.class));
    serializers.put(Set.class, new CollectionSerializer<HashSet>(HashSet.class));
    serializers.put(HashSet.class, new CollectionSerializer<HashSet>(HashSet.class));
  }

  private static class FieldSerializer implements IFieldSerializer<Object> {

    @Override
    public void write(Object object, Class<?> clazz, SimpleOutputStream sos) throws IOException {
      if (!clazz.isPrimitive())
        if (!writeHeader(object, clazz, sos))
          return;
      @SuppressWarnings("unchecked")
      ISimpleSerializer<Object> contentSerializer = (ISimpleSerializer<Object>) serializers.get(object.getClass());
      if (contentSerializer != null)
        contentSerializer.write(sos, object);
      else if (clazz.getComponentType() != null)
        serializeArray(object, sos, clazz);
      else
        serializeNonPrimitive(object, sos);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object read(SimpleInputStream sis, Class<?> clazz) throws IOException {
      Class<?> realClazz = clazz;
      if (!clazz.isPrimitive()) {
        realClazz = readHeader(sis, clazz);
        if (realClazz == null)
          return null;
      }

      ISimpleSerializer<Object> contentSerializer = (ISimpleSerializer<Object>) serializers.get(realClazz);
      if (contentSerializer != null)
        return contentSerializer.read(sis);
      else if (realClazz.getComponentType() != null)
        return deserializeArray(sis, realClazz);
      else
        return deserializeNonPrimitive(sis, realClazz);
    }

    private boolean writeHeader(Object val, Class<?> clazz, SimpleOutputStream sos) throws IOException {
      if (val == null) {
        sos.write((byte) 0);
        return false;
      }
      if (val.getClass() == clazz) {
        sos.write((byte) 1);
      } else {
        sos.write((byte) 2);
        sos.write(val.getClass().getName());
      }
      return true;
    }

    private void serializeArray(Object object, SimpleOutputStream sos, Class<?> clazz) throws IOException {
      Object[] arr = (Object[]) object;
      sos.write(arr.length);
      for (int i = 0; i < arr.length; i++) {
        Object val = arr[i];
        write(val, clazz, sos);
      }
    }

    private void serializeNonPrimitive(Object object, SimpleOutputStream sos) throws IOException {
      try {
        Class<?> clazz = object.getClass();
        for (Field field : getAllFields(clazz)) {
          if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0) {
            field.setAccessible(true);
            write(field.get(object), field.getType(), sos);
          }
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    private Class<?> readHeader(SimpleInputStream sis, Class<?> clazz) throws IOException {
      byte hdr = sis.readByte();
      if (hdr == 0)
        return null;
      else if (hdr == 1)
        return clazz;
      else if (hdr == 2)
        try {
          return Class.forName(sis.readString());
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      throw new RuntimeException("Invalid hdr");
    }

    private Object deserializeArray(SimpleInputStream sis, Class<?> clazz) throws IOException {
      if (sis.readByte() == 0)
        return null;
      int length = sis.readInt();
      if (length < 0)
        throw new RuntimeException("Invalid array length");
      Class<?> componentType = clazz.getComponentType();
      Object[] instance = (Object[]) Array.newInstance(componentType, length);
      for (int i = 0; i < length; i++) {
        instance[i] = read(sis, componentType);
      }
      return instance;
    }

    private <T> T deserializeNonPrimitive(SimpleInputStream sis, Class<T> clazz) throws IOException {
      try {

        Constructor<T> constructor = clazz.getDeclaredConstructor();
        constructor.setAccessible(true);
        T object = constructor.newInstance();

        for (Field field : getAllFields(clazz)) {
          if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0) {
            field.setAccessible(true);
            Object value = read(sis, field.getType());
            field.set(object, value);
          }
        }

        return object;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  private static class HashMapSerializer implements ISimpleSerializer<HashMap<?, ?>> {

    @Override
    public void write(SimpleOutputStream sos, HashMap<?, ?> object) throws IOException {
      sos.write(object.size());
      for (Entry<?, ?> e : object.entrySet()) {
        fieldSerializer.write(e.getKey(), Object.class, sos);
        fieldSerializer.write(e.getValue(), Object.class, sos);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public HashMap<?, ?> read(SimpleInputStream sis) throws IOException {
      HashMap hm = new HashMap();
      int len = sis.readInt();
      for (int i = 0; i < len; i++) {
        Object key = fieldSerializer.read(sis, Object.class);
        Object value = fieldSerializer.read(sis, Object.class);
        hm.put(key, value);
      }
      return hm;
    }

  }
  
  private static class CollectionSerializer<T extends Collection> implements ISimpleSerializer<T> {
    
    private final Class<T> clazz;

    CollectionSerializer(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public void write(SimpleOutputStream sos, T val) throws IOException {
      sos.write(val.size());
      for (Object element : val) {
        fieldSerializer.write(element, Object.class, sos);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T read(SimpleInputStream sis) throws IOException {
      Collection instance = null;
      try {
        instance = clazz.newInstance();
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
      int len = sis.readInt();
      for (int i = 0; i < len; i++) {
        Object value = fieldSerializer.read(sis, Object.class);
        instance.add(value);
      }
      return (T) instance;
    }

   

  }
}
