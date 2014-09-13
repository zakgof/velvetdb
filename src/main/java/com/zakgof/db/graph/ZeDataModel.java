package com.zakgof.db.graph;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.zakgof.db.graph.datadef.BiMultiLinkDef;
import com.zakgof.db.graph.datadef.BiSingleLinkDef;
import com.zakgof.db.graph.datadef.IGetter;
import com.zakgof.db.graph.datadef.ILinkDef;
import com.zakgof.db.graph.datadef.IMultiGetter;
import com.zakgof.db.graph.datadef.ISingleGetter;
import com.zakgof.db.graph.datadef.MultiLinkDef;
import com.zakgof.db.graph.datadef.SingleLinkDef;

public class ZeDataModel {

  private final Multimap<Class<?>, IMultiGetter<?, ?>> multis;
  private final Multimap<Class<?>, ISingleGetter<?, ?>> singles;
  private final Map<String, Class<?>> entities;
  private Map<String, ILinkDef<?, ?>> allLinks;

  public ZeDataModel(Map<String, Class<?>> entities, Multimap<Class<?>, ISingleGetter<?, ?>> singles, Multimap<Class<?>, IMultiGetter<?, ?>> multis, Map<String, ILinkDef<?, ?>> allLinks) {
    this.entities = entities;
    this.singles = singles;
    this.multis = multis;
    this.allLinks = allLinks;
  }

  public Class<?> getEntity(String name) {
    return entities.get(name);
  }

  public ILinkDef<?, ?> getLink(String edgeKind) {
    return allLinks.get(edgeKind);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final Map<String, Class<?>> entities = Maps.newHashMap();
    private final Multimap<Class<?>, ISingleGetter<?, ?>> singles = ArrayListMultimap.create();
    private final Multimap<Class<?>, IMultiGetter<?, ?>> multis = ArrayListMultimap.create();
    private final Map<String, ILinkDef<?, ?>> allLinks = Maps.newHashMap();

    public Builder entities(Class<?>... entities) {
      this.entities.putAll(Arrays.stream(entities).collect(Collectors.toMap(cl -> PersisterUtil.kindOf(cl), cl -> cl)));
      return this;
    }

    private <A> void putGetter(ISingleGetter<A, ?> link) {
      this.singles.put(link.getHostClass(), link);
    }

    private <A> void putGetter(IMultiGetter<A, ?> link) {
      this.multis.put(link.getHostClass(), link);
    }

    public Builder link(SingleLinkDef<?, ?> link) {
      putLink(link);
      putGetter(link);
      return this;
    }

    public Builder link(MultiLinkDef<?, ?> link) {
      putLink(link);
      putGetter(link);
      return this;
    }

    public Builder biLink(BiSingleLinkDef<?, ?> link) {
      putLink(link);
      putGetter(link);
      putGetter(link.back());
      return this;
    }

    private void putLink(ILinkDef<?, ?> link) {
      allLinks.put(link.getKind(), link);
    }

    public Builder biLink(BiMultiLinkDef<?, ?> link) {
      putLink(link);
      putGetter(link);
      putGetter(link.back());
      return this;
    }

    public ZeDataModel build() {
      return new ZeDataModel(entities, singles, multis, allLinks);
    }

  }

  public Collection<String> entityNames() {
    return entities.keySet();
  }

  public ILinkProvider getLinks(Class<?> clazz) {
    return new ILinkProvider() {

      @Override
      public Collection<ISingleGetter<?, ?>> singles() {
        return singles.get(clazz);
      }

      @Override
      public Collection<IMultiGetter<?, ?>> multis() {
        return multis.get(clazz);
      }

      @Override
      public IGetter<?, ?> get(String edgeKind) {
        Optional<ISingleGetter<?, ?>> opSingle = singles.get(clazz).stream().filter(l -> l.getKind().equals(edgeKind)).findAny();
        if (opSingle.isPresent())
          return opSingle.get();
        Optional<IMultiGetter<?, ?>> opMulti = multis.get(clazz).stream().filter(l -> l.getKind().equals(edgeKind)).findAny();
        return opMulti.get();
      }
    };

  }

  public interface ILinkProvider {
    Collection<ISingleGetter<?, ?>> singles();

    Collection<IMultiGetter<?, ?>> multis();

    IGetter<?, ?> get(String edgeKind);
  }

}
