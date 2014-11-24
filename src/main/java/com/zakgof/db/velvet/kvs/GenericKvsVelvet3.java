package com.zakgof.db.velvet.kvs;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import com.zakgof.db.kvs.IKvs;
import com.zakgof.db.kvs.ITransactionalKvs;
import com.zakgof.db.velvet.IRawVelvet;
import com.zakgof.tools.generic.Functions;
import com.zakgof.tools.generic.IFunction;

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

  private final IFunction<IKvs, IIndex> indexer;

  public GenericKvsVelvet3(ITransactionalKvs kvs, Map<String, ?> parameters) {
    this.kvs = kvs;
    this.parameters = parameters;
    this.indexer = kvss -> new MixedIndex(kvss);
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

  private <K> void addToIndex(Object key, K indexentry) {
    indexer.get(kvs).add(key, indexentry);
  }

  private <K> boolean removeFromIndex(Object key, K indexentry) {
    return indexer.get(kvs).remove(key, indexentry);
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
      kvs.put(key, index.toArray((K[]) Array.newInstance(clazz, 0)));
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
    return indexer.get(kvs).getAll(keyClass, nodesKey(kind));
  }

  @Override
  public <T, K> List<T> links(Class<T> clazz, Class<K> keyClass, Object key, String edgekind, String kind) {
    List<T> childNodes = linkKeys(keyClass, key, edgekind).stream().map(linkkey -> get(clazz, kind, linkkey)).collect(Collectors.toList());
    return childNodes;
  }

  @Override
  public <K> List<K> linkKeys(Class<K> clazz, Object key, String edgeKind) {
    Object indexKey = linkDestKey(edgeKind, key);
    return indexer.get(kvs).getAll(clazz, indexKey);   
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
    return indexer.get(kvs).getAll(keyClass, linkOriginsKey(edgeKind));
  }

  /*
   * Checks:
   * 
   * L0 - all nodes are of correct class
   * 
   * L1 - all nodes are of correct kind - in-node key is same as node key
   */

  interface IIndex {
    <K> void add(Object key, K indexentry);

    <K> boolean remove(Object key, K indexentry);

    <K> List<K> getAll(Class<K> clazz, Object key);
  }

  abstract static class AIndex implements IIndex {
    protected IKvs kvss;

    public AIndex(IKvs kvss) {
      this.kvss = kvss;
    }
  }

  static class ArrayIndex extends AIndex {

    public ArrayIndex(IKvs kvss) {
      super(kvss);
    }

    @Override
    public <K> void add(Object key, K indexentry) {
      Class<K> indexEntryClazz = (Class<K>) indexentry.getClass();
      List<K> nodes = getAll(indexEntryClazz, key);
      if (nodes == null)
        nodes = new ArrayList<>();
      if (!nodes.contains(indexentry)) {
        nodes.add(indexentry);
        saveIndex(key, nodes, indexEntryClazz);
      }
    }

    @Override
    public <K> boolean remove(Object key, K indexentry) {
      Class<K> indexEntryClazz = (Class<K>) indexentry.getClass();
      List<K> nodes = getAll(indexEntryClazz, key);
      nodes.remove(indexentry);
      saveIndex(key, nodes, indexEntryClazz);
      return !nodes.isEmpty();
    }

    @Override
    public <K> List<K> getAll(Class<K> clazz, Object key) {
      K[] index = kvss.get(GenericKvsVelvet3.<K> getArrayClass(clazz), key);
      return (index == null) ? new ArrayList<>() : new ArrayList<>(Arrays.asList(index));
    }

    private <K> void saveIndex(Object key, List<K> index, Class<K> clazz) {
      if (index.isEmpty())
        kvss.delete(key);
      else
        kvss.put(key, index.toArray((K[]) Array.newInstance(clazz, 0)));
    }

  }

  static class MixedIndex extends AIndex {

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

    public MixedIndex(IKvs kvss) {
      this(kvss, null, 0, "+");
    }

    public MixedIndex(IKvs kvss, Object origKey, int hashLevel, String indexPath) {
      super(kvss);
      this.hashLevel = hashLevel;
      this.origKey = origKey;
      this.indexPath = indexPath;
    }

    private <K> void addAll(Object key, Class<K> clazz, K[] entries) {
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
    public <K> void add(Object key, K indexentry) {
      MixedInfo info = kvss.get(MixedInfo.class, key);
      if (info == null)
        info = new MixedInfo();
      int bucketIndex = getBucketIndex(indexentry);
      Object bucketKey = bucketKey(origKey == null ? key : origKey, bucketIndex);
      Class<K> indexEntryClazz = (Class<K>) indexentry.getClass();

      if (info.isArray(bucketIndex)) {
        K[] entries = kvss.get(GenericKvsVelvet3.<K> getArrayClass(indexEntryClazz), bucketKey);
        if (Functions.contains(entries, indexentry))
          return;
        if (entries.length + 1 > MAX_ARRAY_BUCKET) {
          // Array -> Hash
          MixedIndex bucketMixedIndexer = childIndexer(key, bucketIndex);
          System.out.println("migrate " + bucketKey);
          K[] newEntries = Arrays.copyOf(entries, entries.length + 1);
          newEntries[entries.length] = indexentry;
          bucketMixedIndexer.addAll(bucketKey, indexEntryClazz, newEntries);
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
        MixedIndex bucketMixedIndexer = childIndexer(key, bucketIndex);
        bucketMixedIndexer.add(bucketKey, indexentry);
      } else {
        // Empty -> Array
        K[] newEntries = Functions.newArray(indexEntryClazz, indexentry);
        kvss.put(bucketKey, newEntries);
        info.setArray(bucketIndex);
        kvss.put(key, info);
      }
    }

    private <K> int getBucketIndex(K indexentry) {
      return (Math.abs(indexentry.hashCode()) >> (hashLevel * HASH_BITS)) % BUCKETS;
    }

    private Object bucketKey(Object key, int bucketIndex) {
      return KeyGen.key("@h/" + indexPath  + bucketIndex + "+", key);
    }
    
    private MixedIndex childIndexer(Object key, int bucketIndex) {
      return new MixedIndex(kvss, origKey == null ? key : origKey, hashLevel + 1, indexPath + bucketIndex + "+");
    }

    @Override
    public <K> boolean remove(Object key, K indexentry) {
      MixedInfo info = kvss.get(MixedInfo.class, key);
      if (info == null)
        throw new NoSuchElementException();
      int bucketIndex = getBucketIndex(indexentry);
      Object bucketKey = bucketKey(origKey == null ? key : origKey, bucketIndex);
      Class<K> indexEntryClazz = (Class<K>) indexentry.getClass();

      if (info.isArray(bucketIndex)) {

        K[] entries = kvss.get(GenericKvsVelvet3.<K> getArrayClass(indexEntryClazz), bucketKey);
        if (entries == null)
          throw new NoSuchElementException();

        K[] newEntries = Functions.newArray(indexEntryClazz, entries.length - 1);
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
        MixedIndex bucketMixedIndexer = childIndexer(key, bucketIndex);
        if (!bucketMixedIndexer.remove(bucketKey, indexentry)) {
          removeBucket(key, info, bucketIndex);
        }
        // TODO : shrink hash to simple array. Keep count ?
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
    public <K> List<K> getAll(Class<K> clazz, Object key) {
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
          MixedIndex bucketMixedIndexer = childIndexer(key, i);
          Collection<K> bucketEntries = bucketMixedIndexer.getAll(clazz, bucketKey);
          result.addAll(bucketEntries);
        }
      }
      return result;
    }

    public <K> void dumpIndex(Object key, Class<K> entryClass, int indent) {
      MixedInfo info = kvss.get(MixedInfo.class, key);
      for (int i=0; i<BUCKETS; i++) {
        Object bucketKey = bucketKey(origKey == null ? key : origKey, i);

        System.err.print(spaces(indent) + "Bucket " + i + " ");
        if (info.isArray(i)) {
            K[] entries = kvss.get(GenericKvsVelvet3.<K> getArrayClass(entryClass), bucketKey);
            System.err.println("Array [" + entries.length + "]");
        } else if (info.isHash(i)) {
          MixedIndex bucketMixedIndexer = childIndexer(key, i);
          System.err.println("Hash");
          bucketMixedIndexer.dumpIndex(bucketKey, entryClass, indent + 2);
        } else {
          System.err.println("Empty");
        }
      }
      
    }

    private String spaces(int len) {
      // TODO Auto-generated method stub
      return "                                   ".substring(0, len);
    }
  }
  
  public <K> void dumpIndex(Class<K> clazz, Object key) {
    ((MixedIndex)indexer.get(kvs)).dumpIndex(key, clazz, 0);
  }

}
