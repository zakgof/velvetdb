package com.zakgof.db.velvet.kvs;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.zakgof.db.kvs.IKvs;
import com.zakgof.db.kvs.ITransactionalKvs;
import com.zakgof.db.velvet.IRawVelvet;
import com.zakgof.tools.generic.Functions;

/**
 * Hardcoded keys:
 * 
 * kvs["@k"] -> Set<String> ["kind1", "kind2", ...] kvs["@e"] -> Set<String> ["edgekind1", "edgekind2", ...] kvs["@n/kind1"] -> Set<kind1keyclass> [node1key, node2key, ...] kvs["@o/edgekind1"] -> Set<linkoriginkeyclass> [linkoriginkey1,
 * linkoriginkey2, ...] kvs["@d/edgekind1/ * ORIGKEY"] -> Set<linkdestkeyclass> [linkoriginkey1, linkoriginkey2, ...]
 * 
 * kvs[@/kind1/ * KEY] -> nodevalue
 * 
 */
public class GenericKvsVelvet3 implements IRawVelvet {
  
  interface IIndex<K> {
    void add(K indexentry);
    boolean remove(K indexentry);
    boolean contains(K indexentry);
    List<K> getAll();
  }

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

  public GenericKvsVelvet3(ITransactionalKvs kvs, Map<String, ?> parameters) {
    this.kvs = kvs;
    this.parameters = parameters;
  }

  public GenericKvsVelvet3(ITransactionalKvs kvs) {
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
    new MixedIndex<K>(kvs, key, (Class<K>)indexentry.getClass()).add(indexentry);
  }

  @SuppressWarnings("unchecked")
  private <K> boolean removeFromIndex(Object key, K indexentry) {
    return new MixedIndex<K>(kvs, key, (Class<K>)indexentry.getClass()).remove(indexentry);
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T[]> getArrayClass(Class<T> clazz) {
    try {
      return (Class<T[]>) Class.forName("[L" + clazz.getName() + ";");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
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
    return new MixedIndex<K>(kvs, nodesKey(kind), keyClass).getAll();
  }
  
  @Override
  public <T> ISortedIndexLink index(Object key1, String edgekind, Comparator<T> comparator, Class<T> clazz, String kind) {
    // TODO
    return null;
  }
  
  @Override
  public ILink index(Object key1, String edgekind, LinkType type) {
    // TODO Auto-generated method stub
    if (type == LinkType.Single)
      return new SingleLink(key1, edgekind);
    else
      return new MultiLink(key1, edgekind);
  }
  
  private class MultiLink implements ILink {

    private Object key1;
    private String edgeKind;

    public MultiLink(Object key1, String edgeKind) {
      this.key1 = key1;
      this.edgeKind = edgeKind;
    }

    @Override
    public void connect(Object key2) {
      addToIndex(EDGEKINDS_KEY, edgeKind);
      addToIndex(linkOriginsKey(edgeKind), key1);
      addToIndex(linkDestKey(edgeKind, key1), key2);
    }

    @Override
    public void disconnect(Object key2) {
      // TODO : locking ?
      if (!removeFromIndex(linkDestKey(edgeKind, key1), key2))
        if (!removeFromIndex(linkOriginsKey(edgeKind), key1))
          removeFromIndex(EDGEKINDS_KEY, edgeKind);
    }

    @Override
    public <K> List<K> linkKeys(Class<K> clazz) {
      return new MixedIndex<K>(kvs, linkDestKey(edgeKind, key1), clazz).getAll();  
    }
    
  }
  
  private class SingleLink implements ILink {

    private Object key1;
    private String edgeKind;

    public SingleLink(Object key1, String edgeKind) {
      this.key1 = key1;
      this.edgeKind = edgeKind;
    }

    @Override
    public void connect(Object key2) {
      addToIndex(EDGEKINDS_KEY, edgeKind);
      addToIndex(linkOriginsKey(edgeKind), key1);
      // TODO : test for existing ?
      kvs.put(linkDestKey(edgeKind, key1), key2);
    }

    @Override
    public void disconnect(Object key2) {
      // TODO : locking ?
     // TODO : optional check
      Object key2ref = kvs.get(key2.getClass(), linkDestKey(edgeKind, key1));
      if (!key2.equals(key2))
        throw new E 
      
        if (!removeFromIndex(linkOriginsKey(edgeKind), key1))
          removeFromIndex(EDGEKINDS_KEY, edgeKind);
    }

    @Override
    public <K> List<K> linkKeys(Class<K> clazz) {
      return new MixedIndex<K>(kvs, linkDestKey(edgeKind, key1), clazz).getAll();  
    }
    
  }

//  @Override
//  public <T, K> List<T> links(Class<T> clazz, Class<K> keyClass, Object key, String edgekind, String kind) {
//    List<T> childNodes = linkKeys(keyClass, key, edgekind).stream().map(linkkey -> get(clazz, kind, linkkey)).collect(Collectors.toList());
//    return childNodes;
//  }

//  @Override
//  public <K> List<K> linkKeys(Class<K> clazz, Object key, String edgeKind) {
//    return new MixedIndex<K>(kvs, linkDestKey(edgeKind, key), clazz).getAll();   
//  }

  // /////////////////////////////////////////////////////////////////////

//
//  @Override
//  public void disconnect(Object key1, Object key2, String edgeKind) {
//    // TODO : locking ?
//    if (!removeFromIndex(linkDestKey(edgeKind, key1), key2))
//      if (!removeFromIndex(linkOriginsKey(edgeKind), key1))
//        removeFromIndex(EDGEKINDS_KEY, edgeKind);
//  }

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
    return new MixedIndex<K>(kvs, linkOriginsKey(edgeKind), keyClass).getAll();
  }

  /*
   * Checks:
   * 
   * L0 - all nodes are of correct class
   * 
   * L1 - all nodes are of correct kind - in-node key is same as node key
   */
  
  @Override
  public <K> IIndex<K> getIndex(Object key, Class<K> clazz) {
    return new MixedIndex<K>(kvs, key, clazz);
  }
  
  @Override
  public <K> ISortedIndex<K> getSortedIndex(Object key, Class<K> clazz, Comparator<K> comparator) {
    // TODO 
    return null;
  }

  public <K> void dumpIndex(Class<K> clazz, Object key) {
    new MixedIndex<K>(kvs, key, clazz).dumpIndex(0);
  }

  static class ArrayIndex<K> implements IIndex<K> {

    private IKvs kvs;
    private Object key;
    private Class<K> clazz;

    public ArrayIndex(IKvs kvss, Class<K> clazz, Object key) {
      this.kvs = kvss;
      this.key = key;
      this.clazz = clazz;
    }

    @Override
    public void add(K indexentry) {
      List<K> nodes = getAll();
      if (nodes == null)
        nodes = new ArrayList<>();
      if (!nodes.contains(indexentry)) {
        nodes.add(indexentry);
        saveIndex(nodes);
      }
    }

    @Override
    public boolean remove(K indexentry) {
      List<K> nodes = getAll();
      nodes.remove(indexentry);
      saveIndex(nodes);
      return !nodes.isEmpty();
    }

    @Override
    public List<K> getAll() {
      K[] index = load();
      return (index == null) ? new ArrayList<>() : Arrays.asList(index);
    }

    protected K[] load() {
      return kvs.get(GenericKvsVelvet3.<K> getArrayClass(clazz), key);
    }

    @SuppressWarnings("unchecked")
    private void saveIndex(List<K> index) {
      if (index.isEmpty())
        kvs.delete(key);
      else
        kvs.put(key, index.toArray((K[]) Array.newInstance(clazz, 0)));
    }

    @Override
    public boolean contains(K indexentry) {
      K[] index = load();
      if (index == null)
        return false;
      return Functions.contains(index, indexentry);
    }

  }
  
  static class SortedArrayIndex<K> extends ArrayIndex<K> implements ISortedIndex<K> {
    
    public SortedArrayIndex(IKvs kvss, Class<K> clazz, Object key) {
      super(kvss, clazz, key);
    }

    @Override
    public K first() {
      K[] index = load();
      return index == null ? null : index[0];
    }

    @Override
    public K last() {
      K[] index = load();
      return index == null ? null : index[index.length - 1];
    }

    @Override
    public K next(K value) {
      K[] index = load();
      if (index == null)
        return null;
      int pos = Functions.indexOf(index, value);
      if (pos < 0 || pos + 1 >= index.length)
        return null;      
      return index == null ? null : index[pos + 1];
    }

    @Override
    public K prev(K value) {
      K[] index = load();
      if (index == null)
        return null;
      int pos = Functions.indexOf(index, value);
      if (pos <= 0)
        return null;      
      return index == null ? null : index[pos - 1];
    }

    @Override
    public K greater(K value) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public K lower(K value) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public K floor(K value) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public K ceiling(K value) {
      // TODO Auto-generated method stub
      return null;
    }

   
  }

  static class MixedIndex<K> implements IIndex<K> {

    private static class MixedInfo {
      public byte bucketArray;
      public byte bucketHashed;

      public boolean isArray(int i) {
        return (bucketArray & (1 << i)) != 0;
      }

      public boolean isHash(int i) {
        return (bucketHashed & (1 << i)) != 0;
      }

      public void setArray(int i) {
        bucketArray |= (1 << i);
      }

      public void arrayToHash(int i) {
        bucketArray &= ~(1 << i);
        bucketHashed |= (1 << i);
      }

      public void setEmpty(int i) {
        bucketArray &= ~(1 << i);
        bucketHashed &= ~(1 << i);
      }

      public boolean isEmpty() {
        return bucketArray == 0 && bucketHashed == 0;
      }
    }

    private static final int HASH_BITS = 3;
    private static final int BUCKETS = 1 << HASH_BITS;
    private static final int MAX_ARRAY_BUCKET = 64;

    private final int hashLevel;
    private final Object origKey;
    private final String indexPath;
    private Class<K> clazz;
    private Object key;
    private IKvs kvss;

    public MixedIndex(IKvs kvss, Object key, Class<K> clazz) {
      this(kvss, key, clazz, null, 0, "+");
    }

    private MixedIndex(IKvs kvss, Object key, Class<K> clazz, Object origKey, int hashLevel, String indexPath) {
      this.kvss = kvss;
      this.hashLevel = hashLevel;
      this.origKey = origKey;
      this.indexPath = indexPath;
      this.clazz = clazz;
      this.key = key;
    }

    private void addAll(K[] entries) {
      MixedInfo info = new MixedInfo();
      @SuppressWarnings("unchecked")
      List<K>[] buckets = new List[BUCKETS];
      for (K entry : entries) {
        int bucketIndex = getBucketIndex(entry);
        List<K> list = buckets[bucketIndex];
        if (list == null) {
          list = new LinkedList<>();
          buckets[bucketIndex] = list;
          info.setArray(bucketIndex);
        }
        list.add(entry);
      }
      for (int i = 0; i < BUCKETS; i++) {
        if (info.isArray(i)) {
          K[] array = Functions.toArray(clazz, buckets[i]);
          kvss.put(bucketKey(origKey, i), array);
        }
      }
      kvss.put(key, info);
    }

    @Override
    public void add(K indexentry) {
      MixedInfo info = kvss.get(MixedInfo.class, key);
      if (info == null)
        info = new MixedInfo();
      int bucketIndex = getBucketIndex(indexentry);
      Object bucketKey = bucketKey(origKey == null ? key : origKey, bucketIndex);

      if (info.isArray(bucketIndex)) {
        K[] entries = kvss.get(GenericKvsVelvet3.<K> getArrayClass(clazz), bucketKey);
        if (Functions.contains(entries, indexentry))
          return;
        if (entries.length + 1 > MAX_ARRAY_BUCKET) {
          // Array -> Hash
          MixedIndex<K> bucketMixedIndexer = childIndexer(bucketKey, key, bucketIndex);
          System.out.println("migrate " + bucketKey);
          K[] newEntries = Arrays.copyOf(entries, entries.length + 1);
          newEntries[entries.length] = indexentry;
          bucketMixedIndexer.addAll(newEntries);
          info.arrayToHash(bucketIndex);
          kvss.put(key, info);
        } else {
          // Array
          K[] newEntries = Arrays.copyOf(entries, entries.length + 1);
          newEntries[entries.length] = indexentry;
          kvss.put(bucketKey, newEntries);
          // System.err.println("- array - " + bucketKey + " " + newEntries.length);
        }
      } else if (info.isHash(bucketIndex)) {
        // Hash
        MixedIndex<K> bucketMixedIndexer = childIndexer(bucketKey, key, bucketIndex);
        bucketMixedIndexer.add(indexentry);
      } else {
        // Empty -> Array
        K[] newEntries = Functions.newArray(clazz, indexentry);
        kvss.put(bucketKey, newEntries);
        info.setArray(bucketIndex);
        kvss.put(key, info);
      }
    }

    private int getBucketIndex(K indexentry) {
      return (Math.abs(indexentry.hashCode()) >> (hashLevel * HASH_BITS)) % BUCKETS;
    }

    private Object bucketKey(Object key, int bucketIndex) {
      return KeyGen.key("@h/" + indexPath  + bucketIndex + "+", key);
    }
    
    private MixedIndex<K> childIndexer(Object bucketKey, Object key, int bucketIndex) {
      return new MixedIndex<K>(kvss, bucketKey, clazz, origKey == null ? key : origKey, hashLevel + 1, indexPath + bucketIndex + "+");
    }

    @Override
    public boolean remove(K indexentry) {
      MixedInfo info = kvss.get(MixedInfo.class, key);
      if (info == null)
        throw new NoSuchElementException();
      int bucketIndex = getBucketIndex(indexentry);
      Object bucketKey = bucketKey(origKey == null ? key : origKey, bucketIndex);

      if (info.isArray(bucketIndex)) {

        K[] entries = kvss.get(GenericKvsVelvet3.<K> getArrayClass(clazz), bucketKey);
        if (entries == null)
          throw new NoSuchElementException();

        K[] newEntries = Functions.newArray(clazz, entries.length - 1);
        int j = 0;
        for (int i = 0; i < entries.length; i++) {
          if (!entries[i].equals(indexentry)) {
            newEntries[j++] = entries[i];
            // TODO throw new NoSuchElementException()
          }
        }
        if (j == 0) {
          kvss.delete(bucketKey);
          removeBucket(key, info, bucketIndex);
        } else {
          kvss.put(bucketKey, newEntries);
        }

      } else if (info.isHash(bucketIndex)) {
        MixedIndex<K> bucketMixedIndexer = childIndexer(bucketKey, key, bucketIndex);
        if (!bucketMixedIndexer.remove(indexentry)) {
          removeBucket(key, info, bucketIndex);
        }
        // TODO : shrink hash back to simple array. Keep count ?
      } else {
        throw new NoSuchElementException();
      }

      return !info.isEmpty();
    }

    
    private void removeBucket(Object key, MixedInfo info, int bucketIndex) {
      info.setEmpty(bucketIndex);
      kvss.put(key, info);
      if (info.isEmpty())
        kvss.delete(key);
    }

    @Override
    public List<K> getAll() {
      List<K> result = new ArrayList<>();

      MixedInfo info = kvss.get(MixedInfo.class, key);
      if (info == null)
        return result;

      for (int i = 0; i < BUCKETS; i++) {
        Object bucketKey = bucketKey(origKey == null ? key : origKey, i);
        if (info.isArray(i)) {
          K[] entries = kvss.get(GenericKvsVelvet3.<K> getArrayClass(clazz), bucketKey);
          for (K entry : entries)
            result.add(entry);
        } else if (info.isHash(i)) {
          MixedIndex<K> bucketMixedIndexer = childIndexer(bucketKey, key, i);
          Collection<K> bucketEntries = bucketMixedIndexer.getAll();
          result.addAll(bucketEntries);
        }
      }
      return result;
    }

    public void dumpIndex(int indent) {
      MixedInfo info = kvss.get(MixedInfo.class, key);
      for (int i=0; i<BUCKETS; i++) {
        Object bucketKey = bucketKey(origKey == null ? key : origKey, i);

        System.err.print(spaces(indent) + "Bucket " + i + " ");
        if (info.isArray(i)) {
            K[] entries = kvss.get(GenericKvsVelvet3.<K> getArrayClass(clazz), bucketKey);
            System.err.println("Array [" + entries.length + "]");
        } else if (info.isHash(i)) {
          MixedIndex<K> bucketMixedIndexer = childIndexer(bucketKey, key, i);
          System.err.println("Hash");
          bucketMixedIndexer.dumpIndex(indent + 2);
        } else {
          System.err.println("Empty");
        }
      }
      
    }

    private String spaces(int len) {
      return "                                   ".substring(0, len);
    }

    @Override
    public boolean contains(K indexentry) {
      // TODO Auto-generated method stub
      return false;
    }
   
  }

}
