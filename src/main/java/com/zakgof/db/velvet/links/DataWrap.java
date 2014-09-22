package com.zakgof.db.velvet.links;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataWrap<T> {

  public static class Builder<T> {

    private final T node;
    private final Map<String, List<DataWrap<?>>> multis = new HashMap<String, List<DataWrap<?>>>();
    private final Map<String, DataWrap<?>> singles = new HashMap<String, DataWrap<?>>();

    public Builder(T node) {
      this.node = node;
    }

    public void addList(String name, List<DataWrap<?>> wrapperLinks) {
      multis.put(name, wrapperLinks);
    }

    public void add(String name, DataWrap<?> childWrap) {
      singles.put(name, childWrap);

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
  
  public DataWrap<?> single(String name) {
    return singles.get(name);
  }
  
  @SuppressWarnings("unchecked")
  public <L> L singleNode(SingleLinkDef<T, L> linkDef) {
    return (L)singles.get(linkDef.getKind()).getNode();
  }

}
