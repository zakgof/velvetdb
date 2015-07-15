package com.zakgof.db.velvet;

import static com.zakgof.db.velvet.VelvetUtil.keyClassOf;
import static com.zakgof.db.velvet.VelvetUtil.keyOf;
import static com.zakgof.db.velvet.VelvetUtil.kindOf;

import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.annotation.AutoKey;

public class Velvet implements IVelvet {

  private final IRawVelvet raw;

  public Velvet(IRawVelvet raw) {
    this.raw = raw;
  }

  @Override
  public <T> List<T> allOf(Class<T> clazz) {
    String kind = kindOf(clazz);
    return raw.allKeys(kind, keyClassOf(clazz)).stream().map(key -> raw.get(clazz, kind, key)).collect(Collectors.toList());
  }

  @Override
  public void delete(Object node) {
    raw.delete(kindOf(node.getClass()), keyOf(node));
  }

  @Override
  public <T> T get(Class<T> clazz, Object key) {
    T node = raw.get(clazz, kindOf(clazz), key);
    return node;
  }

  @Override
  public void put(Object node) {
    String kind = kindOf(node.getClass());
    genZeroAutoKey(node, kind);
    Object key = keyOf(node);
    raw.put(kind, key, node);
    // putLinks(key);
  }

  @Override
  public IRawVelvet raw() {
    return raw;
  }

  private void genZeroAutoKey(Object node, String kind) {
    try {
      Class<?> clazz = node.getClass();
      for (Field field : VelvetUtil.getAllFields(clazz)) {
        field.setAccessible(true);
        if (field.getAnnotation(AutoKey.class) != null) {
          if (field.get(node) == null) {
            field.set(node, field.getType().cast(genUniqueId(kind, clazz)));
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Long genUniqueId(String kind, Class<?> clazz) {
    for (;;) {
      Long key = -UUID.randomUUID().getLeastSignificantBits(); // TODO : lock and check
      if (get(clazz, key) == null)
        return key;
    }
  }

}
