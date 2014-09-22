package com.zakgof.db.velvet.links;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetUtil;

public class IslandModel {

  private final Map<String, Entity<?>> entities;

  public IslandModel(Map<String, Entity<?>> entities) {
    this.entities = entities;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final Map<String, Entity<?>> entities = new HashMap<String, Entity<?>>();

    public <T> EntityBuilder<T> entity(Class<T> clazz) {
      return new EntityBuilder<T>(clazz);
    }

    public class EntityBuilder<T> {

      private final Class<T> clazz;
      private final Map<String, IMultiLinkDef<T, ?>> multis = new HashMap<>();
      private final Map<String, ISingleLinkDef<T, ?>> singles = new HashMap<>();
      private final List<IBiLinkDef<T, ?>> detaches = new ArrayList<>();

      public EntityBuilder(Class<T> clazz) {
        this.clazz = clazz;
      }

      public <L> EntityBuilder<T> include(IMultiLinkDef<T, L> linkDef) {
        multis.put(linkDef.getKind(), linkDef);
        return this;
      }

      public <L> EntityBuilder<T> include(ISingleLinkDef<T, L> linkDef) {
        singles.put(linkDef.getKind(), linkDef);
        return this;
      }

      public <P> EntityBuilder<T> detach(IBiLinkDef<T, P> out) {
        this.detaches.add(out);
        return this;
      }

      public Builder done() {
        Builder.this.addEntity(new Entity<T>(clazz, multis, singles, detaches));
        return Builder.this;
      }

    }

    private <T> void addEntity(Entity<T> entity) {
      entities.put(VelvetUtil.kindOf(entity.clazz), entity);
    }

    public IslandModel build() {
      return new IslandModel(entities);
    }

  }

  private static class Entity<T> {

    private final Class<T> clazz;
    private final Map<String, IMultiLinkDef<T, ?>> multis;
    private final Map<String, ISingleLinkDef<T, ?>> singles;
    private final List<? extends IBiLinkDef<T, ?>> detaches;

    private Entity(Class<T> clazz, Map<String, IMultiLinkDef<T, ?>> multis, Map<String, ISingleLinkDef<T, ?>> singles, List<? extends IBiLinkDef<T, ?>> detaches) {
      this.clazz = clazz;
      this.multis = multis;
      this.singles = singles;
      this.detaches = detaches;
    }

    private Collection<IMultiLinkDef<T, ?>> multies() {
      return multis.values();
    }

    private Collection<ISingleLinkDef<T, ?>> singles() {
      return singles.values();
    }

  }

  public <T> List<DataWrap<T>> fetchAll(IVelvet velvet, Class<T> clazz) {
    List<DataWrap<T>> wrap = velvet.allOf(clazz).stream().map(node -> createWrap(velvet, node)).collect(Collectors.toList());
    return wrap;
  }

  public <T> DataWrap<T> createWrap(IVelvet velvet, T node) {
    DataWrap.Builder<T> wrapBuilder = new DataWrap.Builder<T>(node);
    @SuppressWarnings("unchecked")
    Entity<T> entity = (Entity<T>) entities.get(VelvetUtil.kindOf(node.getClass()));
    if (entity != null) {
      for (IMultiLinkDef<T, ?> multi : entity.multies()) {
        List<?> links = multi.links(velvet, node);
        List<DataWrap<?>> wrapperLinks = new ArrayList<DataWrap<?>>();
        for (Object link : links)
          wrapperLinks.add(createWrap(velvet, link));
        wrapBuilder.addList(multi.getKind(), wrapperLinks);
      }
      for (ISingleLinkDef<T, ?> single : entity.singles()) {
        Object link = single.single(velvet, node);
        DataWrap<?> childWrap = createWrap(velvet, link);
        wrapBuilder.add(single.getKind(), childWrap);
      }
    }
    return wrapBuilder.build();
  }

  public <T> void deleteByKey(IVelvet velvet, Object key, Class<T> clazz) {

    String kind = VelvetUtil.kindOf(clazz);
    Entity<?> entity = entities.get(kind);

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
