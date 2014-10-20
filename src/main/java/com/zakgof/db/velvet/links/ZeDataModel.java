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
import com.zakgof.db.velvet.links.index.IndexedMultiLinkDef;

public class ZeDataModel {

  private final Multimap<Class<?>, IMultiLinkDef<?, ?>> multis;
  private final Multimap<Class<?>, ISingleLinkDef<?, ?>> singles;
  private final Map<String, Class<?>> entities;
  private final Map<String, ILinkDef<?, ?>> allLinks;

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
    
    public Builder link(IndexedMultiLinkDef<?, ?, ?> link) {
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

  public <A> ILinkProvider<A> getLinks(Class<A> clazz) {
    return new ILinkProvider<A>() {

      @SuppressWarnings({ "unchecked", "rawtypes" })
      @Override
      public Collection<ISingleLinkDef<A, ?>> singles() {
        return (Collection<ISingleLinkDef<A, ?>>)(Collection)singles.get(clazz);
      }

      @SuppressWarnings({ "unchecked", "rawtypes" })
      @Override
      public Collection<IMultiLinkDef<A, ?>> multis() {
        return (Collection)multis.get(clazz);
      }
      
      @SuppressWarnings({ "rawtypes", "unchecked" })
      @Override
      public Collection<ILinkDef<A,?>> declaredLinks() {
        return (Collection)allLinks.values();
      }

      @Override
      public ILinkDef<A, ?> get(String edgeKind) {
        Optional<ISingleLinkDef<A, ?>> opSingle = singles().stream().filter(l -> l.getKind().equals(edgeKind)).findAny();
        if (opSingle.isPresent())
          return opSingle.get();
        Optional<IMultiLinkDef<A, ?>> opMulti = multis().stream().filter(l -> l.getKind().equals(edgeKind)).findAny();
        return opMulti.get();
      }
    };

  }

  public interface ILinkProvider<A> {
    Collection<ISingleLinkDef<A, ?>> singles();

    Collection<IMultiLinkDef<A, ?>> multis();
    
    ILinkDef<A, ?> get(String edgeKind);

    Collection<ILinkDef<A,?>> declaredLinks();
  }

}
