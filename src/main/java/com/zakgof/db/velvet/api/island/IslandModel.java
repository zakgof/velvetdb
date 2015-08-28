package com.zakgof.db.velvet.api.island;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.entity.impl.Entities;
import com.zakgof.db.velvet.api.link.IBiLinkDef;
import com.zakgof.db.velvet.api.link.IMultiLinkDef;
import com.zakgof.db.velvet.api.link.ISingleLinkDef;
import com.zakgof.db.velvet.api.link.Links;
import com.zakgof.tools.generic.Functions;
import com.zakgof.tools.generic.IFunction;

public class IslandModel {

  private final Map<String, FetcherEntity<?, ?>> entities;

  public IslandModel(Map<String, FetcherEntity<?, ?>> entities) {
    this.entities = entities;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final Map<String, FetcherEntity<?, ?>> entities = new HashMap<String, FetcherEntity<?, ?>>();

    public <K, V> FetcherEntityBuilder<K, V> entity(IEntityDef<K, V> entityDef) {
      return new FetcherEntityBuilder<K, V>(entityDef);
    }

    public class FetcherEntityBuilder<K, V> {

      private final IEntityDef<K, V> entityDef;
      private final Map<String, IMultiLinkDef<K, V, ?, ?>> multis = new HashMap<>();
      private final Map<Class<?>, IFunction<?, ? extends Comparable<?>>> sorts = new HashMap<>();
      private final Map<String, ISingleLinkDef<K, V, ?, ?>> singles = new HashMap<>();
      private final Map<String, IContextSingleGetter<?>> singleContexts = new HashMap<>();
      private final Map<String, IContextMultiGetter<?>> multiContexts = new HashMap<>();
      private final List<IBiLinkDef<K, V, ?, ?, ?>> detaches = new ArrayList<>();

      public FetcherEntityBuilder(IEntityDef<K, V> entityDef) {
        this.entityDef = entityDef;
      }

      public <L, C extends Comparable<C>> FetcherEntityBuilder<K, V> include(IMultiLinkDef<K, V, ?, L> linkDef, IFunction<L, C> sortBy) {
        multis.put(linkDef.getKind(), linkDef);
        sorts.put(linkDef.getChildEntity().getValueClass(), sortBy);
        return this;
      }

      public <L> FetcherEntityBuilder<K, V> include(IMultiLinkDef<K, V, ?, L> linkDef) {
        multis.put(linkDef.getKind(), linkDef);
        return this;
      }

      public <L> FetcherEntityBuilder<K, V> include(ISingleLinkDef<K, V, ?, L> linkDef) {
        singles.put(linkDef.getKind(), linkDef);
        return this;
      }

      public <P> FetcherEntityBuilder<K, V> detach(IBiLinkDef<K, V, ?, P, ?> out) {
        this.detaches.add(out);
        return this;
      }

      public <L> FetcherEntityBuilder<K, V> include(String name, IContextSingleGetter<L> contextSingleGetter) {
        singleContexts.put(name, contextSingleGetter);
        return this;
      }

      public <L> FetcherEntityBuilder<K, V> include(String name, IContextMultiGetter<L> contextMultiGetter) {
        multiContexts.put(name, contextMultiGetter);
        return this;
      }

      public Builder done() {
        Builder.this.addEntity(new FetcherEntity<K, V>(entityDef, multis, singles, singleContexts, multiContexts, detaches, sorts));
        return Builder.this;
      }

    }

    private <K, V> void addEntity(FetcherEntity<K, V> entity) {
      entities.put(entity.entityDef.getKind(), entity);
    }

    public IslandModel build() {
      return new IslandModel(entities);
    }

  }

  private static class FetcherEntity<K, V> {

    private final IEntityDef<K, V> entityDef;
    private final Map<String, IMultiLinkDef<K, V, ?, ?>> multis;
    private final Map<String, ISingleLinkDef<K, V, ?, ?>> singles;
    private final Map<String, IContextSingleGetter<?>> singleContexts;
    private final Map<String, IContextMultiGetter<?>> multiContexts;
    private final List<? extends IBiLinkDef<K, V, ?, ?, ?>> detaches;
    private final Map<Class<?>, IFunction<?, ? extends Comparable<?>>> sorts;

    private FetcherEntity(IEntityDef<K, V> entityDef, Map<String, IMultiLinkDef<K, V, ?, ?>> multis, Map<String, ISingleLinkDef<K, V, ?, ?>> singles, Map<String, IContextSingleGetter<?>> singleContexts,
                          Map<String, IContextMultiGetter<?>> multiContexts, List<? extends IBiLinkDef<K, V, ?, ?, ?>> detaches, Map<Class<?>, IFunction<?, ? extends Comparable<?>>> sorts) {
      this.entityDef = entityDef;
      this.multis = multis;
      this.singles = singles;
      this.singleContexts = singleContexts;
      this.multiContexts = multiContexts;
      this.detaches = detaches;
      this.sorts = sorts;
    }

  }

  public <T> List<DataWrap<T>> fetchAll(IVelvet velvet, IEntityDef<?, T> entityDef) {
    List<DataWrap<T>> wrap = entityDef.get(velvet).stream().map(node -> this.<T> createWrap(velvet, this.entityOf(node), node, new Context())).collect(Collectors.toList());
    return wrap;
  }

  public <T> DataWrap<T> createWrap(IVelvet velvet, T node) {
    return createWrap(velvet, entityOf(node), node, new Context());
  }

  @SuppressWarnings("unchecked")
  private <T> IEntityDef<?, T> entityOf(T node) {
    return Entities.anno((Class<T>) node.getClass());
  }

  private <T> DataWrap<T> createWrap(IVelvet velvet, IEntityDef<?, T> entityDef, T node, Context context) {
    context.add(node);
    DataWrap.Builder<T> wrapBuilder = new DataWrap.Builder<T>(node);
    @SuppressWarnings("unchecked")
    FetcherEntity<?, T> entity = (FetcherEntity<?, T>) entities.get(entityOf(node).getKind());
    if (entity != null) {
      for (Entry<String, ? extends IMultiLinkDef<?, T, ?, ?>> entry : entity.multis.entrySet()) {
        Stream<?> stream = entry.getValue().multi(velvet, node).stream().filter(l -> l != null);// TODO : check for error
        List<DataWrap<?>> wrappedLinks = decorateBySort(entity, stream, entry.getValue().getChildEntity().getValueClass()).map(o -> createWrap(velvet, entityOf(o), o, context)).collect(Collectors.toList());
        wrapBuilder.addList(entry.getKey(), wrappedLinks);
      }
      for (Entry<String, IContextMultiGetter<?>> entry : entity.multiContexts.entrySet()) {
        IContextMultiGetter<?> getter = entry.getValue();
        Stream<?> stream = getter.multi(velvet, context);
        List<DataWrap<?>> wrappedLinks = stream.map(o -> createWrap(velvet, entityOf(o), o, context)).collect(Collectors.toList());
        wrapBuilder.addList(entry.getKey(), wrappedLinks);
      }
      for (Entry<String, ? extends ISingleLinkDef<?, T, ?, ?>> entry : entity.singles.entrySet()) {
        Object link = entry.getValue().single(velvet, node);
        if (link != null) {
          DataWrap<?> childWrap = createWrap(velvet, entityOf(link), link, context);
          wrapBuilder.add(entry.getKey(), childWrap);
        }
      }
      for (Entry<String, IContextSingleGetter<?>> entry : entity.singleContexts.entrySet()) {
        Object link = entry.getValue().single(velvet, (IIslandContext) context);
        if (link != null) {
          DataWrap<?> childWrap = createWrap(velvet, entityOf(link), link, context);
          wrapBuilder.add(entry.getKey(), childWrap);
        }
      }
    }
    return wrapBuilder.build();
  }

  // Natural sorting
  @SuppressWarnings("unchecked")
  private <T> Stream<?> decorateBySort(FetcherEntity<?, T> entity, Stream<?> stream, Class<?> childClass) {
    IFunction<?, ? extends Comparable<?>> sorter = entity.sorts.get(childClass);
    return (sorter != null) ? stream.sorted(Functions.comparator((IFunction) sorter)) : stream;
  }

  public <K, V> void deleteByKey(IVelvet velvet, K key, IEntityDef<K, V> entityDef) {
    String kind = entityDef.getKind();
    @SuppressWarnings("unchecked")
    FetcherEntity<K, V> entity = (FetcherEntity<K, V>) entities.get(kind);
    if (entity != null) {
      for (IMultiLinkDef<K, V, ?, ?> multi : entity.multis.values())
        dropChildren(velvet, key, multi);
      for (ISingleLinkDef<K, V, ?, ?> single : entity.singles.values())
        dropChild(velvet, key, single);
      for (IBiLinkDef<K, V, ?, ?, ?> detach : entity.detaches)
        detach(velvet, key, detach);
    }
    entityDef.deleteKey(velvet, key);
  }

  public <K, V> void deleteAll(IVelvet velvet, IEntityDef<K, V> entityDef) {
    List<K> keys = entityDef.keys(velvet);
    for (K key : keys)
      deleteByKey(velvet, key, entityDef);
  }

  private <HK, HV, CK, CV> void dropChild(IVelvet velvet, HK key, ISingleLinkDef<HK, HV, CK, CV> single) {
    CK childKey = single.singleKey(velvet, key);
    if (childKey != null) {
      single.disconnectKeys(velvet, key, childKey);
      deleteByKey(velvet, childKey, single.getChildEntity());
    }
  }

  private <HK, HV, CK, CV> void dropChildren(IVelvet velvet, HK key, IMultiLinkDef<HK, HV, CK, CV> multi) {
    List<CK> childrenKeys = multi.multiKeys(velvet, key);
    for (CK childKey : childrenKeys) {
      multi.disconnectKeys(velvet, key, childKey);
      deleteByKey(velvet, childKey, multi.getChildEntity());
    }
  }

  private <HK, HV, CK, CV> void detach(IVelvet velvet, HK key, IBiLinkDef<HK, HV, CK, CV, ?> detach) {
    List<CK> parentKeys = Links.toMultiGetter(detach).multiKeys(velvet, key);
    for (CK parentKey : parentKeys)
      detach.disconnectKeys(velvet, key, parentKey);
  }

  public static <K, V> List<DataWrap<V>> rawRetchAll(IVelvet velvet, IEntityDef<K, V> entityDef) {
    List<DataWrap<V>> nodes = entityDef.get(velvet).stream().map(node -> new DataWrap<V>(node)).collect(Collectors.toList());
    return nodes;
  }

  public <K, V> DataWrap<V> getByKey(IVelvet velvet, K key, IEntityDef<K, V> entityDef) {
    V node = entityDef.get(velvet, key);
    if (node == null)
      return null;
    return createWrap(velvet, entityDef, node, new Context());
  }

  public <T> void save(IVelvet velvet, Collection<DataWrap<T>> data) {
    for (DataWrap<T> wrap : data)
      save(velvet, wrap);
  }

  public <T> void save(IVelvet velvet, DataWrap<T> data) {
    T node = data.getNode();
    IEntityDef<?, T> entityDef = entityOf(node);
    entityDef.put(velvet, node);
    String kind = entityDef.getKind();
    @SuppressWarnings("unchecked")
    FetcherEntity<?, T> entity = (FetcherEntity<?, T>) entities.get(kind);
    if (entity != null) {
      for (ISingleLinkDef<?, T, ?, ?> single : entity.singles.values())
        saveChild(velvet, data, single);
      for (IMultiLinkDef<?, T, ?, ?> multi : entity.multis.values())
        saveChildren(velvet, data, multi);
    }
  }

  private <T, B> void saveChild(IVelvet velvet, DataWrap<T> parentWrap, ISingleLinkDef<?, T, ?, B> singleLink) {
    DataWrap<B> childWrap = parentWrap.singleLink(singleLink);
    singleLink.connect(velvet, parentWrap.getNode(), childWrap.getNode());
    save(velvet, childWrap);
  }

  private <T, B> void saveChildren(IVelvet velvet, DataWrap<T> parentWrap, IMultiLinkDef<?, T, ?, B> multiLink) {
    List<DataWrap<B>> childrenWraps = parentWrap.multiLink(multiLink);
    for (DataWrap<B> childWrap : childrenWraps) {
      multiLink.connect(velvet, parentWrap.getNode(), childWrap.getNode());
      save(velvet, childWrap);
    }
  }

  public interface IIslandContext {
    public <T> T get(Class<T> clazz);
  }

  private class Context implements IIslandContext {

    private final Map<Class<?>, Object> map = new HashMap<>();

    public void add(Object node) {
      map.put(node.getClass(), node);
    }

    @Override
    public <T> T get(Class<T> clazz) {
      return clazz.cast(map.get(clazz));
    }

  }

}
