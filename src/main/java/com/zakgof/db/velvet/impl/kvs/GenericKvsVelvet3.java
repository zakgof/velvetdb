package com.zakgof.db.velvet.impl.kvs;

/**
 * Hardcoded keys:
 * 
 * kvs["@k"] -> Set<String> ["kind1", "kind2", ...] 
 * kvs["@e"] -> Set<String> ["edgekind1", "edgekind2", ...] 
 * kvs["@n/kind1"] -> Set<kind1keyclass> [node1key, node2key, ...] 
 * kvs["@o/edgekind1"] -> Set<linkoriginkeyclass> [linkoriginkey1, * linkoriginkey2, ...] 
 * kvs["@d/edgekind1/ORIGKEY"] -> Set<linkdestkeyclass> [linkoriginkey1, linkoriginkey2, ...] <- array or BTree or whatevar
 * 
 * kvs[@/kind1/KEY] -> nodevalue
 * 
 


public class GenericKvsVelvet3 implements IVelvet {

  interface IIndex<K> {
    void add(K indexentry);

    boolean remove(K indexentry);

    boolean contains(K indexentry);

    List<K> getAll();
  }

  private final IKvs kvs;

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
    
    if (key == null)
      throw new RuntimeException("Velvet: null key"); // TODO      

    addToIndex(KINDS_KEY, kind);
    addToIndex(nodesKey(kind), key);

    Object nodeKey = valueKey(kind, key);
    kvs.put(nodeKey, value);
  }

  @SuppressWarnings("unchecked")
  private <K> void addToIndex(Object key, K indexentry) {
    new MixedIndex<K>(kvs, key, (Class<K>) indexentry.getClass()).add(indexentry);
  }

  @SuppressWarnings("unchecked")
  private <K> boolean removeFromIndex(Object key, K indexentry) {
    return new MixedIndex<K>(kvs, key, (Class<K>) indexentry.getClass()).remove(indexentry);
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
  public <K> List<K> allKeys(String kind, Class<K> keyClass) {
    return new MixedIndex<K>(kvs, nodesKey(kind), keyClass).getAll();
  }

  @Override
  public <K, T, M extends Comparable<M>> IKeyIndexLink<K, M> secondaryKeyIndex(Object key1, String edgekind, Class<T> nodeClazz, String nodekind, Function<T, M> nodeMetric) {  
    return new SortedLink<K, T, M>(key1, edgekind, nodeClazz, nodekind, nodeMetric);
  }
  
  @Override
  public <K extends Comparable<K>, T> IKeyIndexLink<K, K> primaryKeyIndex(Object key1, String edgekind) {
    return new SortedLink<K, T, K>(key1, edgekind, null, null, null);
  }
  

  @Override
  public <K> ILink<K> simpleIndex(Object key1, String edgekind, LinkType type) {
    // TODO Auto-generated method stub
    if (type == LinkType.Single)
      return new SingleLink<K>(key1, edgekind);
    else
      return new MultiLink<K>(key1, edgekind);
  }

  private class BaseLink {

    protected Object key1;
    protected String edgeKind;
    protected Object indexKey;

    protected BaseLink(Object key1, String edgeKind) {
      this.key1 = key1;
      this.edgeKind = edgeKind;
      this.indexKey = linkDestKey(edgeKind, key1);
    }

    protected void preConnect() {
      // TODO: is this needed ?
      addToIndex(EDGEKINDS_KEY, edgeKind);
      addToIndex(linkOriginsKey(edgeKind), key1);
    }

    protected void disconnect() {
      if (!removeFromIndex(linkOriginsKey(edgeKind), key1))
        removeFromIndex(EDGEKINDS_KEY, edgeKind);
    }
    
  }

  private class MultiLink<K> extends BaseLink implements ILink<K> {

    MultiLink(Object key1, String edgeKind) {
      super(key1, edgeKind);
    }

    @Override
    public void put(K key2) {
      if (key2 == null)
        throw new RuntimeException("Velvet: null key"); // TODO
      preConnect();
      addToIndex(indexKey, key2);
    }

    @Override
    public void delete(K key2) {
      // TODO : locking ?
      if (!removeFromIndex(indexKey, key2))
        disconnect();
    }
    
    @Override
    public boolean contains(K bkey) {
      return new MixedIndex<K>(kvs, indexKey, (Class<K>)bkey.getClass()).contains(bkey);
    }

    @Override
    public List<K> keys(Class<K> clazz) {
      return new MixedIndex<K>(kvs, indexKey, clazz).getAll();
    }

  }

  private class SingleLink<K> extends BaseLink implements ILink<K> {

    SingleLink(Object key1, String edgeKind) {
      super(key1, edgeKind);
    }

    @Override
    public void put(K key2) {
      preConnect();
      // TODO : test for existing ?
      kvs.put(indexKey, key2);
    }

    @Override
    public void delete(K key2) {
      // TODO : locking ?
      // TODO : optional check
      // Object key2ref = kvs.get(key2.getClass(), indexKey);
      // if (!key2.equals(key2ref))
      //   throw new NoSuchElementException();
      // kvs.delete(indexKey);

      disconnect();
    }

    @Override
    public List<K> keys(Class<K> clazz) {
      return Arrays.asList(kvs.get(clazz, indexKey));
    }
    
    @Override
    public boolean contains(K bkey) {      
      return bkey.equals(kvs.get(bkey.getClass(), indexKey));
    }

  }

  private class SortedLink<K, T, M extends Comparable<M>> extends BaseLink implements IKeyIndexLink<K, M> {

    private final Function<K, M> keyMetric;

    SortedLink(Object key1, String edgeKind, Class<T> nodeClazz, String nodekind, Function<T, M> nodeMetric) {
      super(key1, edgeKind);
      this.keyMetric = nodeMetric == null ? key -> (M)key : key -> nodeMetric.apply(getValue(nodeClazz, nodekind, key));
    }

    private T getValue(Class<T> nodeClazz, String nodekind, K key) {
      T val = GenericKvsVelvet3.this.get(nodeClazz, nodekind, key);
      if (val == null)
        System.err.println("Fatal : metric value not found : " + nodeClazz + "," + nodekind + "," + key);
      return val;
    }

    @Override
    public void put(K key2) {
      preConnect();
      @SuppressWarnings("unchecked")
      Class<K> clazz = (Class<K>) key2.getClass();
      K[] index = kvs.get(GenericKvsVelvet3.<K> getArrayClass(clazz), indexKey);
      if (index == null)
        index = Functions.newArray(clazz, 0);      
      int insertIndex = searchForInsert(index, keyMetric.apply(key2), true);
      index = ArrayUtil.insert(index, key2, insertIndex); // TODO : duplicates ?
      kvs.put(indexKey, index);
    }
    
    private int searchForInsert(K[] array, M value, boolean last) {
      int i0 = 0;
      int i1 = array.length - 1;
      if (i1 == -1)
        return 0;
      M m0 = keyMetric.apply(array[i0]);
      M m1 = keyMetric.apply(array[i1]);
      if (value.compareTo(m0) < 0 || value.compareTo(m0) == 0 && !last)
        return 0;
      if (value.compareTo(m1) > 0 || value.compareTo(m1) == 0 && last)
        return i1 + 1;
      return searchForInsert(array, value, i0, i1, m0, m1, last);
    }
    
    private int searchForInsert(K[] array, M value, int i0, int i1, M m0, M m1, boolean last) {
      int i = (i0 + i1) / 2;
      M m = keyMetric.apply(array[i]);
      if (i1 - i0 == 1)
        return i1;
      if (value.compareTo(m) > 0 || value.compareTo(m) == 0 && last)
        return searchForInsert(array, value, i, i1, m, m1, last);
      return searchForInsert(array, value, i0, i, m0, m, last);
    }
    
    private int exactSearch(K[] array, K value, Function<K, M> metric) {
      for (int i=0; i<array.length; i++)
        if (array[i].equals(value))
          return i;
      return -1;
      
      -- M valueMetric = metric.apply(value);
      int i = searchForInsert(array, valueMetric, true) - 1;
      for(;;i--) {
        if (i < 0 || valueMetric.compareTo(metric.apply(array[i])) < 0)
          return -1;
        if (array[i].equals(value))
          return i;
      } --
    }

    @Override
    public void delete(K key2) {
      @SuppressWarnings("unchecked")
      Class<K> clazz = (Class<K>) key2.getClass();
      K[] index = kvs.get(GenericKvsVelvet3.<K> getArrayClass(clazz), indexKey);

      int pos = exactSearch(index, key2, keyMetric);
      if (pos < 0)
        throw new NoSuchElementException();
      if (index.length == 1)
        kvs.delete(indexKey);
      else {
        index = ArrayUtil.remove(index, pos);
        kvs.put(indexKey, index);
      }
    }
    
    @Override
    public boolean contains(K bkey) {
      @SuppressWarnings("unchecked")
      Class<K> clazz = (Class<K>) bkey.getClass();
      K[] index = kvs.get(GenericKvsVelvet3.<K> getArrayClass(clazz), indexKey);
      return Functions.contains(index, bkey); // TODO : binary search with metric can make it faster...
    }

    @Override
    public List<K> keys(Class<K> clazz) {
      K[] index = kvs.get(GenericKvsVelvet3.<K> getArrayClass(clazz), indexKey);
      List<K> list = index == null ? new ArrayList<>() : new ArrayList<>(Arrays.asList(index)); // TODO : perf
      return list;
    }

    @Override
    public void update(K key2) {
      delete(key2);
      put(key2); // TODO: performance      
    }

    @Override
    public List<K> keys(Class<K> clazz, IIndexQuery<M> query) {      
      K[] index = kvs.get(GenericKvsVelvet3.<K> getArrayClass(clazz), indexKey);      
      return queryArray(index, query);
    }

    List<K> queryArray(K[] array, IIndexQuery query) {
      
      if (array == null)
        return new ArrayList<>();
      
      int i1 = getLeftIndex(array, query.getLowAnchor());
      int i2 = getRightIndex(array, query.getHighAnchor());
      
      List<K> list = new ArrayList<>();
      if (!query.isAscending()) {
        i2 -= query.getOffset();
        if (query.getLimit() > 0)
          i1 = Math.max(i1, i2 - query.getLimit() + 1);
        for (int i=i2; i>=i1; i--)
          list.add(array[i]);
      } else {
        i1 += query.getOffset();
        if (query.getLimit() > 0)
          i2 = Math.min(i2, i1 + query.getLimit() - 1);
        for (int i=i1; i<=i2; i++)
          list.add(array[i]);
      }
      return list;
    }
    
    *
     * key inclusive: left then scan
     * key exclusive: left then scan
     * value inclusive: left
     * value exclusive: right
     * no value : zero
     * @param index
     * @param anchor
     * @return
     *
    private int getLeftIndex(K[] index, IQueryAnchor<?> anchor) {
//      if (anchor == null)
//        return 0;
//      
//      K key = null;
//      int position = -1;
//      M m1 = null;
//      if (anchor instanceof IKeyAnchor) {
//        key = ((IKeyAnchor<K>)anchor).getKey();
//        m1 = keyMetric.apply(key);
//      } else if (anchor instanceof IPositionAnchor) {
//        position = ((IPositionAnchor)anchor).getPosition();
//        return anchor.isIncluding() ? position : position + 1;
//      } else if (anchor instanceof ISecondaryIndexAnchor) {
//        m1 = ((ISecondaryIndexAnchor<M>)anchor).getValue();
//      }
//      boolean right = !anchor.isIncluding() && key == null;
//      int i1 = searchForInsert(index, m1, right);
//      if (key == null)
//        return i1;
//      
//      for (int i=i1;; i++) {
//        if (i == index.length || keyMetric.apply(index[i]).compareTo(m1) > 0)
//          if (anchor.isIncluding())
//            throw new NoSuchElementException();
//          else
//            return i;
//        if (index[i].equals(key))
//          return anchor.isIncluding() ? i : i + 1;        
//      }
      return 0; // TODO
    }
    
     *
     * key inclusive: right then scan
     * key exclusive: right then scan
     * value inclusive: right
     * value exclusive: left
     * no value : zero
     * @param index
     * @param anchor
     * @return
     
    private int getRightIndex(K[] index, IQueryAnchor anchor) {
      
      /*
      if (anchor == null)
        return index.length - 1;
      
      
      K key = null;
      int position = -1;
      M m2 = null;
      if (anchor instanceof IKeyAnchor) {
        key = ((IKeyAnchor<K>)anchor).getKey();
        m2 = keyMetric.apply(key);
      } else if (anchor instanceof IPositionAnchor) {
        position = ((IPositionAnchor)anchor).getPosition();
        return anchor.isIncluding() ? position : position - 1;
      } else if (anchor instanceof ISecondaryIndexAnchor) {
        m2 = ((ISecondaryIndexAnchor<M>)anchor).getValue();
      }
      boolean right = anchor.isIncluding() || key != null;
      int i1 = searchForInsert(index, m2, right) - 1;
      if (key == null)
        return i1;
      for (int i=i1;; i--) {
        if (i < 0 || keyMetric.apply(index[i]).compareTo(m2) < 0)
          if (anchor.isIncluding())
            throw new NoSuchElementException();
          else
            return i;
        if (index[i].equals(key))
          return anchor.isIncluding() ? i : i - 1;        
      }
      
      return 0; // TODO
    }

  }
  
  
   * 
   * Leaf = 
   *  - keyarray
   *  - next 
   *  - prev
   *  
   * Node = 
   * 
   *  - separator values
   *  - list of children (k+1)
   *    
   * 
   * 
   
//  private class SortedBTreeLink<K, T, M extends Comparable<M>> extends BaseLink implements ISortedIndexLink<K, T, M> {


  @Override
  public void lock(String lockName, long timeout) {
    // TODO Auto-generated method stub

  }

  @Override
  public void unlock(String lockName) {
    // TODO Auto-generated method stub

  }

  @Override
  public void begin() {
    kvs.begin();
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

  
   * Checks:
   * 
   * L0 - all nodes are of correct class
   * 
   * L1 - all nodes are of correct kind - in-key key is same as key key
   

  public <K> void dumpIndex(Class<K> clazz, Object key) {
    new MixedIndex<K>(kvs, key, clazz).dumpIndex(0);
  }

  *
   * Store all links in array
   * pros:
   * - single read/write op
   * cons: 
   * - add/delete - replace the whole array (io too many bytes)
   * - search - read all and linear in-memory seek (memory, performance)
   
  static class ArrayIndex<K> implements IIndex<K> {

    private final IKvs kvs;
    private final Object key;
    private final Class<K> clazz;

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

  *
   * Hash tree implementation
   * kvs["@d/edgekind1/ORIGKEY"] -> MixedInfo
   * @h/bucketpath/ORIGKEY -> array or MixedInfo
   * arrays expands to 8 buckets after size of 64 (never collapses back)
   * 
  
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
          // System.out.println("migrate " + bucketKey);
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
      return KeyGen.key("@h/" + indexPath + bucketIndex + "+", key);
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
        
        // DEBUG. reduce should be 1
        long reduce = Arrays.stream(entries).filter(e -> e.equals(indexentry)).count();
        if (reduce != 1)
          System.err.println("Warning : remove entry reduce " + key + " " + this.origKey + " " + reduce);

        K[] newEntries = Functions.newArray(clazz, entries.length - (int)reduce);
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
      for (int i = 0; i < BUCKETS; i++) {
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
      MixedInfo info = kvss.get(MixedInfo.class, key);
      if (info == null)
        return false;
      int bucketIndex = getBucketIndex(indexentry);
      Object bucketKey = bucketKey(origKey == null ? key : origKey, bucketIndex);

      if (info.isArray(bucketIndex)) {
        K[] entries = kvss.get(GenericKvsVelvet3.<K> getArrayClass(clazz), bucketKey);
        return Functions.contains(entries, indexentry);
      } else if (info.isHash(bucketIndex)) {
        MixedIndex<K> bucketMixedIndexer = childIndexer(bucketKey, key, bucketIndex);
        return bucketMixedIndexer.contains(indexentry);         
      }
      return false;
    }

  }

}
*/