package com.zakgof.db.velvet.links;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetUtil;

public class IslandModel {

  private final Map<String, FetcherEntity<?>> entities;

  public IslandModel(Map<String, FetcherEntity<?>> entities) {
    this.entities = entities;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final Map<String, FetcherEntity<?>> entities = new HashMap<String, FetcherEntity<?>>();

    public <T> FetcherEntityBuilder<T> entity(Class<T> clazz) {
      return new FetcherEntityBuilder<T>(clazz);
    }

    public class FetcherEntityBuilder<T> {

      private final Class<T> clazz;
      private final Map<String, IMultiGetter<T, ?>> multis = new HashMap<>();
      private final Map<String, ISingleGetter<T, ?>> singles = new HashMap<>();
      // private final List<IBiLinkDef<T, ?>> detaches = new ArrayList<>();

      public FetcherEntityBuilder(Class<T> clazz) {
        this.clazz = clazz;
      }
      
      public <L> FetcherEntityBuilder<T> include(String name, IMultiGetter<T, L> linkDef) {
        multis.put(name, linkDef);
        return this;
      }

      public <L> FetcherEntityBuilder<T> include(IMultiLinkDef<T, L> linkDef) {
        multis.put(linkDef.getKind(), linkDef);
        return this;
      }

      public <L> FetcherEntityBuilder<T> include(ISingleLinkDef<T, L> linkDef) {
        singles.put(linkDef.getKind(), linkDef);
        return this;
      }

//      public <P> FetcherEntityBuilder<T> detach(IBiLinkDef<T, P> out) {
//        this.detaches.add(out);
//        return this;
//      }

      public Builder done() {
        Builder.this.addEntity(new FetcherEntity<T>(clazz, multis, singles));
        return Builder.this;
      }

    }

    private <T> void addEntity(FetcherEntity<T> entity) {
      entities.put(VelvetUtil.kindOf(entity.clazz), entity);
    }

    public IslandModel build() {
      return new IslandModel(entities);
    }

  }

  private static class FetcherEntity<T> {

    private final Class<T> clazz;
    private Map<String, IMultiGetter<T, ?>> multis = new HashMap<>();
    private Map<String, ISingleGetter<T, ?>> singles = new HashMap<>();
    // private final List<? extends IBiLinkDef<T, ?>> detaches;

    private FetcherEntity(Class<T> clazz, Map<String, IMultiGetter<T, ?>> multis, Map<String, ISingleGetter<T, ?>> singles) {
      this.clazz = clazz;
      this.multis = multis;
      this.singles = singles;
      // this.detaches = detaches;
    }

  }

  public <T> List<DataWrap<T>> fetchAll(IVelvet velvet, Class<T> clazz) {
    List<DataWrap<T>> wrap = velvet.allOf(clazz).stream().map(node -> createWrap(velvet, node)).collect(Collectors.toList());
    return wrap;
  }

  public <T> DataWrap<T> createWrap(IVelvet velvet, T node) {
    DataWrap.Builder<T> wrapBuilder = new DataWrap.Builder<T>(node);
    @SuppressWarnings("unchecked")
    FetcherEntity<T> entity = (FetcherEntity<T>) entities.get(VelvetUtil.kindOf(node.getClass()));
    if (entity != null) {
      for (Entry<String, ? extends IMultiGetter<T, ?>> entry : entity.multis.entrySet()) {
        List<DataWrap<?>> wrappedLinks = entry.getValue().links(velvet, node).stream().map(o -> createWrap(velvet, o)).collect(Collectors.toList());        
        wrapBuilder.addList(entry.getKey(), wrappedLinks);
      }
      for (Entry<String, ? extends ISingleGetter<T, ?>> entry : entity.singles.entrySet()) {
        Object link = entry.getValue().single(velvet, node);
        DataWrap<?> childWrap = createWrap(velvet, link);
        wrapBuilder.add(entry.getKey(), childWrap);
      }
    }
    return wrapBuilder.build();
  }

  // TODO: reimplement
  public <T> void deleteByKey(IVelvet velvet, Object key, Class<T> clazz) {
    
    /*

    String kind = VelvetUtil.kindOf(clazz);
    FetcherEntity<?> entity = entities.get(kind);

    if (entity != null) {
      for (IMultiLinkDef<?, ?> multi : entity.multies()) {
        if (entity.detaches.contains(multi))
          continue;
        List<?> linkedKeys = multi.linkKeys(velvet, key);
        for (Object linkKey : linkedKeys) {
          multi.disconnectKeys(velvet, key, linkKey);
          deleteByKey(velvet, linkKey, multi.getChildClass());
        }
      }
      for (ISingleLinkDef<?, ?> single : entity.singles()) {
        if (entity.detaches.contains(single))
          continue;
        Object linkKey = single.singleKey(velvet, key);
        if (linkKey != null) {
          single.disconnectKeys(velvet, key, linkKey);
          deleteByKey(velvet, linkKey, single.getChildClass());
        }
      }
      for (IBiLinkDef<?, ?> parent : entity.detaches) {
        List<Object> parentKeys = LinkUtil.toMultiGetter(parent).linkKeys(velvet, key);
        parentKeys.stream().forEach(parentKey -> parent.disconnectKeys(velvet, parentKey, key));
      }
    }
    velvet.raw().delete(kind, key);
    */
  }

  public static <T> List<DataWrap<T>> rawRetchAll(IVelvet velvet, Class<T> clazz) {
    List<DataWrap<T>> nodes = velvet.allOf(clazz).stream().map(node -> new DataWrap<T>(node)).collect(Collectors.toList());
    return nodes;
  }

  public <T> DataWrap<T> getByKey(IVelvet velvet, Object key, Class<T> clazz) {
    T node = velvet.get(clazz, key);
    return createWrap(velvet, node);
  }

}
