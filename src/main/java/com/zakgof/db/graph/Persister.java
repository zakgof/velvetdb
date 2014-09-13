package com.zakgof.db.graph;

import static com.zakgof.db.graph.PersisterUtil.keyOf;
import static com.zakgof.db.graph.PersisterUtil.kindOf;

import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;

import com.zakgof.db.graph.annotation.AutoKey;
import com.zakgof.db.graph.annotation.Link;
import com.zakgof.db.graph.annotation.Link.GetMode;
import com.zakgof.db.graph.annotation.Link.PutMode;

public class Persister implements IPersister {

  private final ISession session;

  public Persister(ISession session) {
    this.session = session;
  }

  @Override
  public <T> T singleLink(Class<T> clazz, Object node, String edgeKind) {
    List<T> links = session.links(clazz, keyOf(node), edgeKind, kindOf(clazz));
    if (links.isEmpty())
      return null;
    if (links.size() > 1) { 
      throw new RuntimeException("Multiple links found in singleLink");
    }
    return links.get(0);
  }

  @Override
  public <T> List<T> links(Class<T> clazz, Object node, String edgeKind) {
    List<T> links = session.links(clazz, keyOf(node), edgeKind, kindOf(clazz));
    for (T linkNode : links)
      getLinks(linkNode);
    return links;
  }
  
  @Override
  public <T> List<T> allOf(Class<T> clazz) {
    List<T> links = session.allOf(clazz, kindOf(clazz));
    for (T linkNode : links)
      getLinks(linkNode);
    return links;
  }

  @Override
  public void delete(Object node) {
    // TODO : delete links
    session.delete(kindOf(node.getClass()), keyOf(node));
  }

  @Override
  public ISession session() {
    return session;
  }

  @Override
  public <T> T get(Class<T> clazz, Object key) {
    T node = session.get(clazz, kindOf(clazz), key);
    if (node != null)
      getLinks(node);
    return node;
  }

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

  @Override
  public void put(Object node) {
    String kind = kindOf(node.getClass());
    genZeroAutoKey(node, kind);
    Object key = keyOf(node);
    session.put(kind, key, node);
    putLinks(node);
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

  private void genZeroAutoKey(Object node, String kind) {
    try {
      Class<?> clazz = node.getClass();
      for (Field field : PersisterUtil.getAllFields(clazz)) {
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
      Long key = UUID.randomUUID().getLeastSignificantBits();
      if (get(clazz, key) == null)
        return key;
    }
  }

  @Override
  public void connect(Object node1, Object node2, String kind) {
    session.connect(keyOf(node1), keyOf(node2), kind);
  }

  @Override
  public void disconnect(Object node1, Object node2, String kind) {    
    session.disconnect(keyOf(node1), keyOf(node2), kind);
  }
 

}
