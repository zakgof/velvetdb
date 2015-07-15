package com.zakgof.db.velvet.old.index;

import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IRawVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetUtil;
import com.zakgof.db.velvet.api.query.IIndexQuery;
import com.zakgof.db.velvet.old.IMultiLinkDef;
import com.zakgof.db.velvet.old.MultiLinkDef;

public class IndexedMultiLinkDef<A, B, C extends Comparable<C>> extends MultiLinkDef<A, B> implements IIndexedMultiLink<A, B, C> {

  private final Function<B, C> metric;

  public IndexedMultiLinkDef(Class<A> aClazz, Class<B> bClazz, String edgeKind, Function<B, C> metric) {
    super(aClazz, bClazz, edgeKind);
    this.metric = metric;
  }

  public static <A, B, C extends Comparable<C>> IndexedMultiLinkDef<A, B, C> of(Class<A> aClazz, Class<B> bClazz, Function<B, C> metrics) {
    return new IndexedMultiLinkDef<A, B, C>(aClazz, bClazz, VelvetUtil.kindOf(aClazz) + "_" + VelvetUtil.kindOf(bClazz), metrics);
  }
  
  private <K> IKeyIndexLink<K> index(IVelvet velvet, Object akey) {
    return velvet.raw().<K, B, C>index(akey, edgeKind, getChildClass(), VelvetUtil.kindOf(getChildClass()), metric);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public List<?> linkKeys(IVelvet velvet, Object akey) {
    return index(velvet, akey).linkKeys((Class<Object>)VelvetUtil.keyClassOf(getChildClass()));
  }
  
  @Override
  public void connectKeys(IVelvet velvet, Object akey, Object bkey) {
    index(velvet, akey).connect(bkey);
  }
  
  @Override
  public void disconnectKeys(IVelvet velvet, Object akey, Object bkey) {
    index(velvet, akey).disconnect(bkey);
  }

  @Override
  public <K> List<B> links(IVelvet velvet, A node, IIndexQuery<K> indexQuery) {
    @SuppressWarnings("unchecked")
    List<K> keys = linkeKeys(velvet, VelvetUtil.keyOf(node), indexQuery);
    return VelvetUtil.getAll(velvet, keys, getChildClass());
  }

  @SuppressWarnings("unchecked")
  private <K> List<K> linkeKeys(IVelvet velvet, Object akey, IIndexQuery<K> indexQuery) {
    return this.<K>index(velvet, akey).linkKeys((Class<K>)VelvetUtil.keyClassOf(getChildClass()), indexQuery);
  }
  
  // TODO : use ALinkDef
  public <K> IMultiLinkDef<A, B> indexGetter(final IIndexQuery<K> indexQuery) {
    return new IndexedMultiLinkDef<A, B, C>(getHostClass(), bClazz, edgeKind, metric) {      
      @Override
      public List<?> linkKeys(IVelvet velvet, Object key) {
        return IndexedMultiLinkDef.this.<K>linkeKeys(velvet, key, indexQuery);
      }      
    };
  }
}
  
  /*

  private class IndexRequest {

    private final List<?> linkKeys;
    private final IVelvet velvet;
    private final IndexStorageRecord store;
    private final Map<Integer, B> childrenCache;
    private final A key;
    private final int size;

    public IndexRequest(IVelvet velvet, A key) {
      this.velvet = velvet;
      this.linkKeys = velvet.raw().linkKeys(VelvetUtil.keyClassOf(getChildClass()), VelvetUtil.keyOf(key), getKind());
      List<IndexStorageRecord> links = velvet.links(IndexStorageRecord.class, key, getIndexLinkName());
      this.store = links.isEmpty() ? new IndexStorageRecord() : links.get(0); // TODO
      this.size = store.indices.size();
      this.childrenCache = new HashMap<>();
      this.node = key;
    }

    private String getIndexLinkName() {
      return "@idx/cool/" + getKind();
    }

    public List<B> run(IndexQuery<B, C> indexQuery) {

      // range [p1, p2)
      int startIndex = find(indexQuery.l1, false);
      if (startIndex >= size)
        return new ArrayList<>();
      int endIndex = find(indexQuery.l2, true);
      if (endIndex < 0)
        return new ArrayList<>();

      int recordCount = indexQuery.limit < 0 ? endIndex - startIndex - indexQuery.offset : Math.min(indexQuery.limit, endIndex - startIndex - indexQuery.offset);
      int[] a = new int[recordCount];

      if (indexQuery.descending) {
        for (int i = 0; i < recordCount; i++)
          a[i] = store.indices.get(endIndex - i);
      } else {
        for (int i = 0; i < recordCount; i++)
          a[i] = store.indices.get(i + indexQuery.offset + startIndex + 1);
      }

      List<B> values = Arrays.stream(a).mapToObj(i -> indexValue(i)).collect(Collectors.toList());
      return values;
    }

    private int find(IndexQuery.Level<B, C> l, boolean upper) {
      
      if (l == null)
         return upper ? size - 1 : -1;
      
      if (l.key != null)
        return findIndexByNode(l, upper);        
      
      
      if (store.indices.isEmpty())
        return -1;
      
      C c1 = indexMetric(0);
      if (less(l.m, c1, l.inclusive ^ upper))
        return -1;

      C c2 = indexMetric(size - 1);
      if (greater(l.m, c2, l.inclusive ^ !upper))
        return size - 1;

      return find(l.m, 0, size - 1, c1, c2, l.inclusive ^ !upper);
    }

    private int findIndexByNode(IndexQuery.Level<B, C> l, boolean upper) {
      int low = find(new Level<B, C>(metric.get(l.key), true), false);
      if (low >= size)
        throwNoNode();
      int high = find(new Level<B, C>(metric.get(l.key), true), true);
      if (high < 0)
        throwNoNode();
      Object key = VelvetUtil.keyOf(l.key);
      for (int i = low + 1; i<=high; i++)
        if (VelvetUtil.keyOf(indexValue(store.indices.get(i))).equals(key))
          return i + (upper ? -1 : 1);
      throwNoNode();
      return -1;
    }

    private void throwNoNode() {
      throw new RuntimeException("Node specified in index query does not exist");      
    }

    private int find(C m, int i1, int i2, C c1, C c2, boolean inclusive) {
      int i = (i1 + i2) / 2;
      if (i == i1 || i == i2)
        return i1;

      C c = indexMetric(i);
      if (greater(m, c, inclusive))
        return find(m, i, i2, c, c2, inclusive);
      else
        return find(m, i1, i, c1, c, inclusive);
    }

    private C indexMetric(int flatindex) {
      return metric.get(indexValue(store.indices.get(flatindex)));
    }

    private B indexValue(int index) {
      B child = childrenCache.get(index);
      if (child != null)
        return child;
      child = velvet.get(getChildClass(), linkKeys.get(index));
      childrenCache.put(index, child);
      return child;
    }

    private boolean greater(C c1, C c2, boolean inclusive) {
      int comp = c1.compareTo(c2);
      return comp > 0 || comp == 0 && inclusive;
    }

    private boolean less(C c1, C c2, boolean inclusive) {
      int comp = c1.compareTo(c2);
      return comp < 0 || comp == 0 && inclusive;
    }

    public void add(B b) {
      C c = metric.get(b);
      int index = find(new Level<>(c, false), true);
      store.indices.add(index + 1, size);
      boolean needLink = (store.getKey() == null);
      velvet.put(store);
      if (needLink)
        velvet.connect(key, store, getIndexLinkName());
    }

    public void remove(B b) {
      int index = findIndexByNode(new Level<B,C>(b), false);
      
      B b2 = indexValue(store.indices.get(index - 1));      
      if (!VelvetUtil.equals(b2,  b))
        throw new IllegalArgumentException(); // FATAL
      
      
      int removedIdx = store.indices.remove(index - 1);
      for (int i = 0; i<store.indices.size(); i++) {
        int oldval = store.indices.get(i);
        if (oldval > removedIdx) {
          oldval--;
          store.indices.set(i, oldval);
        }
      }
      
      velvet.put(store);
    }

  }

  @Override
  public List<B> links(IVelvet velvet, A key, IndexQuery<B, C> indexQuery) {
    List<B> nodes = new IndexRequest(velvet, key).run(indexQuery);
    return nodes;
  }

  // TODO : avoid fetching host key
  public List<Object> linkKeys(IVelvet velvet, Object key, IndexQuery<B, C> indexQuery) {
    List<Object> childKeys = new IndexRequest(velvet, velvet.get(getHostClass(), key)).run(indexQuery).stream().map(key -> VelvetUtil.keyOf(key)).collect(Collectors.toList());
    return childKeys;
  }

  @Override
  public String toString() {
    return "indexed " + super.toString();
  }
 
  @Override
  public void connect(IVelvet velvet, A a, B b) {
    super.connect(velvet, a, b);
    new IndexRequest(velvet, a).add(b);
  }

  @Override
  public void disconnect(IVelvet velvet, A a, B b) {    
    new IndexRequest(velvet, a).remove(b);
    super.disconnect(velvet, a, b);
  }

  @Override
  public void disconnectKeys(IVelvet velvet, Object akey, Object bkey) {
    // TODO

  }

  

  public static void main(String[] args) {

    IVelvet velvet = new Velvet(new GenericKvsVelvet3(new MemKvs()));

    KK parent = new KK();
    velvet.put(parent);

    IndexedMultiLinkDef<KK, T1, Float> link = IndexedMultiLinkDef.of(KK.class, T1.class, kk -> kk.getM());

    T1 t1 = new T1("one", 1.0f);
    T1 t2 = new T1("two", 2.0f);
    T1 t3 = new T1("three", 3.0f);
    T1 t4 = new T1("three-o", 3.0f);
    T1 t5 = new T1("five", 5.0f);

    velvet.put(t1);
    velvet.put(t2);
    velvet.put(t3);
    velvet.put(t4);
    velvet.put(t5);

    link.connect(velvet, parent, t1);
    link.connect(velvet, parent, t5);
    link.connect(velvet, parent, t3);
    link.connect(velvet, parent, t4);
    link.connect(velvet, parent, t2);
    
    
    System.err.println(link.links(velvet, parent, IndexQuery.range(-10.0f, true, 3.5f, false))); // 1 2 3 3
    System.err.println(link.links(velvet, parent, IndexQuery.range(-10.0f, true, 3.0f, false))); // 1 2
    System.err.println(link.links(velvet, parent, IndexQuery.range(-10.0f, true, 3.0f, true))); // 1 2 3 3
    System.err.println(link.links(velvet, parent, IndexQuery.range(2.0f, true, 30.0f, true))); // 2 3 3 5
    System.err.println(link.links(velvet, parent, IndexQuery.range(2.0f, false, 30.0f, true))); // 3 3 5
    System.err.println(link.links(velvet, parent, IndexQuery.range(2.5f, false, 30.0f, true))); // 3 3 5
    System.err.println(link.links(velvet, parent, IndexQuery.range(2.5f, true, 30.0f, true))); // 3 3 5
    System.err.println(link.links(velvet, parent, IndexQuery.range(3.0f, true, 5.0f, false))); // 3 3
    System.err.println(link.links(velvet, parent, IndexQuery.range(3.0f, false, 5.0f, true))); // 5
    System.err.println(link.links(velvet, parent, IndexQuery.greater(3.5f))); // 5
    System.err.println(link.links(velvet, parent, IndexQuery.greater(2.0f))); // 3 3 5
    System.err.println(link.links(velvet, parent, IndexQuery.greaterOrEq(3.5f))); // 5
    System.err.println(link.links(velvet, parent, IndexQuery.greaterOrEq(2.0f))); // 2 3 3 5
    System.err.println(link.links(velvet, parent, IndexQuery.less(2.0f))); // 1
    System.err.println(link.links(velvet, parent, IndexQuery.lessOrEq(2.0f))); // 1 2
    System.err.println(link.links(velvet, parent, IndexQuery.<T1, Float>builder().less(5.0f).descending().limit(35).build())); // 3 3 2 1
    
    link.disconnect(velvet, parent, t3);
    System.err.println(link.links(velvet, parent)); // 3 3 2 1
    
    
    T1 t = LinkUtil.toSingleGetter(link.indexGetter(IndexQuery.<T1, Float>builder().descending().limit(1).build())).single(velvet, parent);
    while(t != null) {
      System.err.println("sequence : " + t);
      t = LinkUtil.toSingleGetter(link.indexGetter(IndexQuery.<T1, Float>builder().lessO(t).descending().limit(1).build())).single(velvet, parent);      
    }
    
    
    
  }

}

class T1 extends AutoKeyed {
  public T1() {
  }

  public T1(String s, float m) {
    this.s = s;
    this.m = m;
  }

  public float getM() {
    return m;
  }

  private float m;
  private String s;

  @Override
  public String toString() {
    return s + " " + m;
  }
}

class KK extends AutoKeyed {
}

*/
