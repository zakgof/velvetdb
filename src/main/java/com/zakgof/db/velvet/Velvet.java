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
  public <T> List<T> links(Class<T> clazz, Object node, String edgeKind) {
    List<T> links = raw.links(clazz, keyClassOf(clazz), keyOf(node), edgeKind, kindOf(clazz));
    return links;
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
    // putLinks(node);
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

  @Override
  public void connect(Object node1, Object node2, String kind) {
    raw.connect(keyOf(node1), keyOf(node2), kind);
  }

  @Override
  public void disconnect(Object node1, Object node2, String kind) {
    raw.disconnect(keyOf(node1), keyOf(node2), kind);
  }
  


  /*
 
 
  private <T> void getLinks(T node) {
    if (node == null)
      return;
    Class<?> clazz = node.getClass();
    try {
      for (Field field : clazz.getDeclaredFields()) {
        Link annotation = field.getAnnotation(Link.class);
        if (annotation != null) {
          if (annotation.get() == GetMode.FETCH) {
            field.setAccessible(true);
            String kind = annotation.edgeKind().isEmpty() ? kindOf(field.getType()) : annotation.edgeKind();
            Object linked = singleLink(field.getType(), node, kind);
            field.set(node, linked);
          }
        }
      }
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
 
  private void putLinks(Object node) {
    Class<?> clazz = node.getClass();
    try {
      for (Field field : clazz.getDeclaredFields()) {
        field.setAccessible(true);
        Link annotation = field.getAnnotation(Link.class);
        if (annotation != null) {
          if (annotation.put() == PutMode.LINK) {
            field.setAccessible(true);
            putLink(node, field.get(node), annotation.edgeKind());
          } else if (annotation.put() == PutMode.BILINK) {
            field.setAccessible(true);
            putBiLink(node, field.get(node), annotation.edgeKind(), annotation.backEdgeKind());
          }
        }
      }
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
 
  private void putLink(Object node, Object linked, String edgeKind) {
    String kind = edgeKind.isEmpty() ? kindOf(linked.getClass()) : edgeKind;
    connect(node, linked, kind);
  }

  private void putBiLink(Object node, Object linked, String edgeKind, String backEdgeKind) {
    String kind = edgeKind.isEmpty() ? kindOf(linked.getClass()) : edgeKind;
    connect(node, linked, kind);
    String backkind = backEdgeKind.isEmpty() ? kindOf(node.getClass()) : backEdgeKind;
    connect(linked, node, backkind);
  }
  
  */

 

}
