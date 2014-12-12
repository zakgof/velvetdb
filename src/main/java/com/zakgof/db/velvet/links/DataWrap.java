package com.zakgof.db.velvet.links;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zakgof.db.velvet.VelvetUtil;

public class DataWrap<T> {

  public static class Builder<T> {

    private final T node;
    private final Map<String, List<DataWrap<?>>> multis = new HashMap<String, List<DataWrap<?>>>();
    private final Map<String, DataWrap<?>> singles = new HashMap<String, DataWrap<?>>();

    public Builder(T node) {
      this.node = node;
    }
    
    public Builder(DataWrap<T> wrap) {
      this.node = wrap.node;
      this.multis.putAll(wrap.multis);
      this.singles.putAll(wrap.singles);
    }

    public Builder<T> addList(String name, List<DataWrap<?>> wrapperLinks) {
      multis.put(name, wrapperLinks);
      return this;
    }

    public Builder<T> add(String name, DataWrap<?> childWrap) {
      singles.put(name, childWrap);
      return this;
    }

    public DataWrap<T> build() {
      return new DataWrap<T>(node, singles, multis);
    }

  }

  private final T node;
  private final Map<String, DataWrap<?>> singles;
  private final Map<String, List<DataWrap<?>>> multis;

  public T getNode() {
    return node;
  }

  public DataWrap(T node, Map<String, DataWrap<?>> singles, Map<String, List<DataWrap<?>>> multis) {
    this.node = node;
    this.singles = singles;
    this.multis = multis;
  }
  
  public DataWrap(T node) {
    this.node = node;
    this.singles = Collections.emptyMap();
    this.multis = Collections.emptyMap();
  }

  public List<DataWrap<?>> multi(String name) {
    return multis.get(name);
  }
  
  @SuppressWarnings("unchecked")
  public <L> List<DataWrap<L>> multiLink(IMultiLinkDef<T, L> link) {
    return (List<DataWrap<L>>)(List<?>)multis.get(link.getKind());
  }
  
  public DataWrap<?> single(String name) {
    return singles.get(name);
  }
  
  @SuppressWarnings("unchecked")
  public <L> DataWrap<L> singleLink(ISingleLinkDef<T, L> link) {
    return (DataWrap<L>) singles.get(link.getKind());
  }
  
  @SuppressWarnings("unchecked")
  public <L> L singleNode(SingleLinkDef<T, L> linkDef) {
    return (L)singles.get(linkDef.getKind()).getNode();
  }
  
  @Override
  public String toString() {
    return " " + node + " " +
        singles.entrySet().stream().reduce("", (s, e) -> s + e.getKey() + " [" + valueString(e.getValue()) + " ]" , (s1, s2) -> s1 + s2) +
        multis.entrySet().stream().reduce("", (s, e) -> s + e.getKey() + " [" + e.getValue().size() + " ]" , (s1, s2) -> s1 + s2);
  }
  
  private String valueString(DataWrap<?> wrap) {
    if (VelvetUtil.isEntity(wrap.getNode()))
        return "" + VelvetUtil.keyOf(wrap.getNode());
    return wrap.getNode().toString();
  }

  public <V> DataWrap<T> attach(String name, V value) {
    return new Builder<>(this).add(name, new DataWrap<V>(value)).build();
  }

}
