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

  static class IndexQuery<C> {
    public C p1;
    public C p2;
  }

  private class IndexRequest {

    private final List<?> linkKeys;
    private final IVelvet velvet;
    private final IndexStorageRecord store;
    private final Map<Integer, B> childrenCache;
    private final A node;

    public IndexRequest(IVelvet velvet, A node) {
      this.velvet = velvet;
      this.linkKeys = velvet.raw().linkKeys(VelvetUtil.keyClassOf(getHostClass()), VelvetUtil.keyOf(node), multiLink.getKind());
      List<IndexStorageRecord> links = velvet.links(IndexStorageRecord.class, node, getIndexLinkName());
      this.store = links.isEmpty() ? new IndexStorageRecord() : links.get(0); // TODO
      this.childrenCache = new HashMap<>();
      this.node = node;
    }

    private String getIndexLinkName() {
      return "@idx/cool/" + multiLink.getKind();
    }

    public List<B> run(IndexQuery<C> indexQuery) {

      // range [p1, p2)
      int startIndex = find(indexQuery.p1);
      if (startIndex >= store.indices.size())
        return new ArrayList<>();
      int endIndex = find(indexQuery.p2);
      if (endIndex < 0)
        return new ArrayList<>();

      int[] a = new int[endIndex - startIndex];

      for (int i = startIndex + 1; i <= endIndex; i++) {
        a[i - startIndex - 1] = store.indices.get(i);
      }
      List<B> values = Arrays.stream(a).mapToObj(i -> indexValue(i)).collect(Collectors.toList());
      return values;
    }

    private int find(C c) {      
      if (store.indices.isEmpty())
        return -1;
      
      C c1 = indexMetric(0);
      if (c.compareTo(c1) < 0)
        return -1;

      C c2 = indexMetric(store.indices.size() - 1);
      if (c.compareTo(c2) > 0)
        return store.indices.size() - 1;

      return ceilingIndex(c, 0, store.indices.size() - 1, c1, c2);
    }

    private int ceilingIndex(C p, int i1, int i2, C c1, C c2) {
      int i = (i1 + i2) / 2;

      if (i == i1 || i == i2)
        return i1;// TODO

      B b = indexValue(store.indices.get(i));
      C c = metric.get(b);
      if (p.compareTo(c) > 0)
        return ceilingIndex(p, i, i2, c, c2);
      else
        return ceilingIndex(p, i1, i, c1, c);
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

    public void add(B b) {
      C c = metric.get(b);
      int index = find(c);
      store.indices.add(index + 1, store.indices.size());
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
    T1 t4 = new T1("four", 4.0f);
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
    
    IndexQuery<Float> query = new IndexQuery<Float>();
    query.p1 = 0.5f;
    query.p2 = 3.5f;
    
    List<T1> list = link.links(velvet, parent, query);
    System.err.println(list);
    
    

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
