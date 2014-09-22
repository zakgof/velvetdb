package com.zakgof.db.velvet.links;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.zakgof.db.velvet.VelvetUtil;

public class ZeDataModel {

  private final Multimap<Class<?>, IMultiLinkDef<?, ?>> multis;
  private final Multimap<Class<?>, ISingleLinkDef<?, ?>> singles;
  private final Map<String, Class<?>> entities;
  private Map<String, ILinkDef<?, ?>> allLinks;

  public ZeDataModel(Map<String, Class<?>> entities, Multimap<Class<?>, ISingleLinkDef<?, ?>> singles, Multimap<Class<?>, IMultiLinkDef<?, ?>> multis, Map<String, ILinkDef<?, ?>> allLinks) {
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
    private final Multimap<Class<?>, ISingleLinkDef<?, ?>> singles = ArrayListMultimap.create();
    private final Multimap<Class<?>, IMultiLinkDef<?, ?>> multis = ArrayListMultimap.create();
    private final Map<String, ILinkDef<?, ?>> allLinks = Maps.newHashMap();

    public Builder entities(Class<?>... entities) {
      this.entities.putAll(Arrays.stream(entities).collect(Collectors.toMap(cl -> VelvetUtil.kindOf(cl), cl -> cl)));
      return this;
    }

    private <A> void putGetter(ISingleLinkDef<A, ?> link) {
      this.singles.put(link.getHostClass(), link);
    }

    private <A> void putGetter(IMultiLinkDef<A, ?> link) {
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

    public Builder link(BiMultiLinkDef<?, ?> link) {
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
      public Collection<ISingleLinkDef<?, ?>> singles() {
        return singles.get(clazz);
      }

      @Override
      public Collection<IMultiLinkDef<?, ?>> multis() {
        return multis.get(clazz);
      }
      
      @Override
      public Collection<ILinkDef<?,?>> declaredLinks() {
        return allLinks.values();
      }

      @Override
      public ILinkDef<?, ?> get(String edgeKind) {
        Optional<ISingleLinkDef<?, ?>> opSingle = singles.get(clazz).stream().filter(l -> l.getKind().equals(edgeKind)).findAny();
        if (opSingle.isPresent())
          return opSingle.get();
        Optional<IMultiLinkDef<?, ?>> opMulti = multis.get(clazz).stream().filter(l -> l.getKind().equals(edgeKind)).findAny();
        return opMulti.get();
      }
    };

  }

  public interface ILinkProvider {
    Collection<ISingleLinkDef<?, ?>> singles();

    Collection<IMultiLinkDef<?, ?>> multis();
    
    ILinkDef<?, ?> get(String edgeKind);

    Collection<ILinkDef<?,?>> declaredLinks();
  }

}
