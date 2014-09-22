package com.zakgof.db.velvet.links;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.zakgof.db.sqlkvs.MemKvs;
import com.zakgof.db.velvet.AutoKeyed;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.Velvet;
import com.zakgof.db.velvet.VelvetUtil;
import com.zakgof.db.velvet.kvs.GenericKvsVelvet2;
import com.zakgof.tools.generic.IFunction;

public class IndexedMultiLinkDef<A, B, C extends Comparable<C>> implements IMultiLinkDef<A, B> {

  private final IFunction<B, C> metric;
  private final MultiLinkDef<A, B> multiLink;

  public IndexedMultiLinkDef(Class<A> aClazz, Class<B> bClazz, String edgeKind, IFunction<B,C> metric) {
    multiLink = MultiLinkDef.of(aClazz, bClazz, edgeKind);
    this.metric = metric;
  }

  public static <A, B, C extends Comparable<C>> IndexedMultiLinkDef<A, B, C> of(Class<A> aClazz, Class<B> bClazz, IFunction<B, C> metrics) {
    return new IndexedMultiLinkDef<A, B, C>(aClazz, bClazz, VelvetUtil.kindOf(aClazz) + "-" + VelvetUtil.kindOf(bClazz), metrics);
  }

  private class IndexRequest {

    private final List<?> linkKeys;
    private final IVelvet velvet;
    private final IndexStorageRecord store;
    private final Map<Integer, B> childrenCache;
    private final A node;
    private int size;

    public IndexRequest(IVelvet velvet, A node) {
      this.velvet = velvet;
      this.linkKeys = velvet.raw().linkKeys(VelvetUtil.keyClassOf(getHostClass()), VelvetUtil.keyOf(node), multiLink.getKind());
      List<IndexStorageRecord> links = velvet.links(IndexStorageRecord.class, node, getIndexLinkName());
      this.store = links.isEmpty() ? new IndexStorageRecord() : links.get(0); // TODO
      this.size = store.indices.size();
      this.childrenCache = new HashMap<>();
      this.node = node;
    }

    private String getIndexLinkName() {
      return "@idx/cool/" + multiLink.getKind();
    }

    public List<B> run(IndexQuery<C> indexQuery) {

      // range [p1, p2)      
      int startIndex = indexQuery.p1 == null ? -1 : find(indexQuery.p1, indexQuery.inclusive1, false);
      if (startIndex >= size)
        return new ArrayList<>();
      int endIndex = indexQuery.p2 == null ? size - 1 : find(indexQuery.p2, indexQuery.inclusive2, true);
      if (endIndex < 0)
        return new ArrayList<>();

      int[] a = new int[endIndex - startIndex];

      for (int i = startIndex + 1; i <= endIndex; i++) {
        a[i - startIndex - 1] = store.indices.get(i);
      }
      List<B> values = Arrays.stream(a).mapToObj(i -> indexValue(i)).collect(Collectors.toList());
      return values;
    }

    private int find(C c, boolean inclusive, boolean upper) {      
      if (store.indices.isEmpty())
        return -1;
      
      C c1 = indexMetric(0);
      if (less(c, c1, inclusive))
        return -1;

      C c2 = indexMetric(size - 1);
      if (greater(c, c2, inclusive))
        return size - 1;

      return find(c, 0, size - 1, c1, c2, inclusive ^ !upper);
    }

    private int find(C p, int i1, int i2, C c1, C c2, boolean inclusive) {
      int i = (i1 + i2) / 2;
      if (i == i1 || i == i2)
        return i1;// TODO
      
      C c = indexMetric(i);
      if (greater(p, c, inclusive))
        return find(p, i, i2, c, c2, inclusive);
      else
        return find(p, i1, i, c1, c, inclusive);
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
      int index = find(c, false, true);
      store.indices.add(index + 1, size);
      boolean needLink = (store.getKey() == null);
      velvet.put(store);
      if (needLink)
        velvet.connect(node, store, getIndexLinkName());
    }

  }

  public List<B> links(IVelvet velvet, A node, IndexQuery indexQuery) {
    List<B> collect = new IndexRequest(velvet, node).run(indexQuery);
    return collect;
  }

  @Override
  public List<Object> linkKeys(IVelvet velvet, Object key) {
    return multiLink.linkKeys(velvet, key);
  }

  @Override
  public String toString() {
    return "indexed " + multiLink;
  }

  @Override
  public String getKind() {
    return multiLink.getKind();
  }

  @Override
  public Class<A> getHostClass() {
    return multiLink.getHostClass();
  }

  @Override
  public Class<B> getChildClass() {
    return multiLink.getChildClass();
  }

  @Override
  public void connect(IVelvet velvet, A a, B b) {
    multiLink.connect(velvet, a, b);
    new IndexRequest(velvet, a).add(b);
  }

  @Override
  public void connectKeys(IVelvet velvet, Object akey, Object bkey) {
    // TODO Auto-generated method stub

  }

  @Override
  public void disconnect(IVelvet velvet, A a, B b) {
    // TODO Auto-generated method stub

  }

  @Override
  public void disconnectKeys(IVelvet velvet, Object akey, Object bkey) {
    // TODO Auto-generated method stub

  }

  @Override
  public List<B> links(IVelvet velvet, A node) {
    // TODO Auto-generated method stub
    return null;
  }

  public static void main(String[] args) {

    IVelvet velvet = new Velvet(new GenericKvsVelvet2(new MemKvs()));

    KK parent = new KK();
    velvet.put(parent);
    
    IndexedMultiLinkDef<KK, T1, Float> link = IndexedMultiLinkDef.of(KK.class, T1.class, kk -> kk.getM());
    
    T1 t1 = new T1("one", 1.0f);
    T1 t2 = new T1("two", 2.0f);
    T1 t3 = new T1("three", 3.0f);
    T1 t4 = new T1("four", 3.0f);
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
    
    
    System.err.println(link.links(velvet, parent, IndexQuery.range(-10.0f, true, 3.5f, false)));    // 1 2 3 3
    System.err.println(link.links(velvet, parent, IndexQuery.range(-10.0f, true, 3.0f, false)));    // 1 2
    System.err.println(link.links(velvet, parent, IndexQuery.range(-10.0f, true, 3.0f, true)));     // 1 2 3 3
    System.err.println(link.links(velvet, parent, IndexQuery.range(2.0f, true, 30.0f, true)));      // 2 3 3 5
    System.err.println(link.links(velvet, parent, IndexQuery.range(2.0f, false, 30.0f, true)));     // 3 3 5
    System.err.println(link.links(velvet, parent, IndexQuery.range(2.5f, false, 30.0f, true)));     // 3 3 5
    System.err.println(link.links(velvet, parent, IndexQuery.range(2.5f, true, 30.0f, true)));      // 3 3 5
    System.err.println(link.links(velvet, parent, IndexQuery.range(3.0f, true, 5.0f, false)));      // 3 3
    System.err.println(link.links(velvet, parent, IndexQuery.range(3.0f, false, 5.0f, true)));      // 5
    System.err.println(link.links(velvet, parent, IndexQuery.greater(3.5f)));                       // 5
    System.err.println(link.links(velvet, parent, IndexQuery.greater(2.0f)));                       // 3 3 5
    System.err.println(link.links(velvet, parent, IndexQuery.greaterOrEq(3.5f)));                   // 5
    System.err.println(link.links(velvet, parent, IndexQuery.greaterOrEq(2.0f)));                   // 2 3 3 5
    System.err.println(link.links(velvet, parent, IndexQuery.less(2.0f)));                          // 1
    System.err.println(link.links(velvet, parent, IndexQuery.lessOrEq(2.0f)));                      // 1 2
    
    
    
    
    

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
