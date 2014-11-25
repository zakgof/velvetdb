package com.zakgof.db.velvet.kvs;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.zakgof.db.kvs.ITransactionalKvs;
import com.zakgof.db.velvet.IRawVelvet;

/**
 * Hardcoded keys:
 * 
 * kvs["@k"] -> Set<String> ["kind1", "kind2", ...] 
 * kvs["@e"] -> Set<String> ["edgekind1", "edgekind2", ...] 
 * kvs["@n/kind1"] -> Set<kind1keyclass> [node1key, node2key, ...] 
 * kvs["@o/edgekind1"] -> Set<linkoriginkeyclass> [linkoriginkey1, linkoriginkey2, ...] 
 * kvs["@d/edgekind1/ * ORIGKEY"] -> Set<linkdestkeyclass> [linkoriginkey1, linkoriginkey2, ...]
 * 
 * kvs[@/kind1/ * KEY] -> nodevalue
 * 
 */
public class GenericKvsVelvet2 implements IRawVelvet {

  private final ITransactionalKvs kvs;

  private static final String KINDS_KEY = "@k";
  private static final String EDGEKINDS_KEY = "@e";

  private static String nodesKey(String kind) {
    return "@n/" + kind;
  }

  private static String linkOriginsKey(String edgeKind) {
    return "@o/" + edgeKind;
  }

  private static Object linkDestKey(String edgeKind, Object origKey) {
    return KeyGen.key("@d/" + edgeKind + "/", origKey);
  }

  private static Object valueKey(String kind, Object key) {
    return KeyGen.key("@/" + kind + "/", key);
  }

  private final Map<String, ?> parameters;

  public GenericKvsVelvet2(ITransactionalKvs kvs, Map<String, ?> parameters) {
    this.kvs = kvs;
    this.parameters = parameters;
  }

  public GenericKvsVelvet2(ITransactionalKvs kvs) {
    this(kvs, Collections.emptyMap());
  }

  @Override
  public <T> T get(Class<T> clazz, String kind, Object key) {
    Object nodeKey = valueKey(kind, key);
    return kvs.get(clazz, nodeKey);
  }

  @Override
  public <T> void put(String kind, Object key, T value) {

    addToIndex(KINDS_KEY, kind);
    addToIndex(nodesKey(kind), key);

    Object nodeKey = valueKey(kind, key);
    kvs.put(nodeKey, value);
  }

  @SuppressWarnings("unchecked")
  private <K> void addToIndex(Object key, K indexentry) {
    Class<K> indexEntryClazz = (Class<K>) indexentry.getClass();
    List<K> nodes = loadIndex(key, indexEntryClazz);
    if (nodes == null)
      nodes = new ArrayList<>();
    if (!nodes.contains(indexentry)) {
      nodes.add(indexentry);
      saveIndex(key, nodes, indexEntryClazz);
    }
  }

  @SuppressWarnings("unchecked")
  private <K> boolean removeFromIndex(Object key, K indexentry) {
    Class<K> indexEntryClazz = (Class<K>) indexentry.getClass();
    List<K> nodes = loadIndex(key, indexEntryClazz);
    nodes.remove(indexentry);
    saveIndex(key, nodes, indexEntryClazz);
    return !nodes.isEmpty();
  }

  private <T> List<T> loadIndex(Object key, Class<T> clazz) {
    T[] index = kvs.get(GenericKvsVelvet2.<T> getArrayClass(clazz), key);
    return (index == null) ? new ArrayList<>() : new ArrayList<>(Arrays.asList(index));
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T[]> getArrayClass(Class<T> clazz) {
    try {
      return (Class<T[]>) Class.forName("[L" + clazz.getName() + ";");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private <K> void saveIndex(Object key, List<K> index, Class<K> clazz) {
    if (index.isEmpty())
      kvs.delete(key);
    else
      kvs.put(key, index.toArray((K[])Array.newInstance(clazz, 0)));
  }

  @Override
  public void delete(String kind, Object key) {
    Object nodeKey = valueKey(kind, key);
    kvs.delete(nodeKey);

    if (!removeFromIndex(nodesKey(kind), key))
      removeFromIndex(KINDS_KEY, kind);
  }

  @Override
  public <K> Collection<K> allKeys(String kind, Class<K> keyClass) {
    return loadIndex(nodesKey(kind), keyClass);
  }

  @Override
  public <T, K> List<T> links(Class<T> clazz, Class<K> keyClass, Object key, String edgekind, String kind) {
    List<T> childNodes = linkKeys(keyClass, key, edgekind).stream().map(linkkey -> get(clazz, kind, linkkey)).collect(Collectors.toList());
    return childNodes;
  }

  @Override
  public <K> List<K> linkKeys(Class<K> clazz, Object key, String edgeKind) {
    Object indexKey = linkDestKey(edgeKind, key);
    List<K> index = loadIndex(indexKey, clazz);
    return index;
  }

  // /////////////////////////////////////////////////////////////////////

  @Override
  public void connect(Object key1, Object key2, String edgeKind) {
    addToIndex(EDGEKINDS_KEY, edgeKind);
    addToIndex(linkOriginsKey(edgeKind), key1);
    addToIndex(linkDestKey(edgeKind, key1), key2);
  }

  @Override
  public void disconnect(Object key1, Object key2, String edgeKind) {
    // TODO : locking ?
    if (!removeFromIndex(linkDestKey(edgeKind, key1), key2))
      if (!removeFromIndex(linkOriginsKey(edgeKind), key1))
        removeFromIndex(EDGEKINDS_KEY, edgeKind);
  }

  @Override
  public void lock(String lockName, long timeout) {
    // TODO Auto-generated method stub

  }

  @Override
  public void unlock(String lockName) {
    // TODO Auto-generated method stub

  }

  @Override
  public void rollback() {
    kvs.rollback();
  }

  @Override
  public void commit() {
    kvs.commit();
  }
  
  public <K> Collection<K> getLinkOriginKeys(String edgeKind, Class<K> keyClass) {
    return loadIndex(linkOriginsKey(edgeKind), keyClass);
  }

  @Override
  public <K> IIndex<K> getIndex(Object key, Class<K> clazz) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <K> ISortedIndex<K> getSortedIndex(Object key, Class<K> clazz, Comparator<K> comparator) {
    // TODO Auto-generated method stub
    return null;
  }
  
  /*
   * Checks:
   * 
   * L0 - all nodes are of correct class
   * 
   * L1 - all nodes are of correct kind - in-node key is same as node key
   */

}
