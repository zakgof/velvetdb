package com.zakgof.db.graph.datadef;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zakgof.db.graph.IPersister;
import com.zakgof.db.graph.PersisterUtil;
import com.zakgof.tools.generic.IFunction;
import com.zakgof.tools.generic.Lists;

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
      private final Map<String, MultiLinkDef<T, ?>> multis = new HashMap<String, MultiLinkDef<T, ?>>();
      private final Map<String, SingleLinkDef<T, ?>> singles = new HashMap<String, SingleLinkDef<T, ?>>();
      private final List<BiMultiLinkDef<?, T>> parents = new ArrayList<BiMultiLinkDef<?, T>>();

      public EntityBuilder(Class<T> clazz) {
        this.clazz = clazz;
      }

      public <L> EntityBuilder<T> with(MultiLinkDef<T, L> linkDef) {
        multis.put(linkDef.getKind(), linkDef);
        return this;
      }

      public <L> EntityBuilder<T> with(SingleLinkDef<T, L> linkDef) {
        singles.put(linkDef.getKind(), linkDef);
        return this;
      }

      public Builder done() {
        Builder.this.addEntity(new Entity<T>(clazz, multis, singles, parents));
        return Builder.this;
      }

      public <P> EntityBuilder<T> parentOf(BiMultiLinkDef<P, T> parent) {
        this.parents.add(parent);
        return this;
      }

    }

    private <T> void addEntity(Entity<T> entity) {
      entities.put(PersisterUtil.kindOf(entity.clazz), entity);
    }

    public IslandModel build() {

      return new IslandModel(entities);
    }

  }

  private static class Entity<T> {

    private final Class<T> clazz;
    private final Map<String, MultiLinkDef<T, ?>> multis;
    private final Map<String, SingleLinkDef<T, ?>> singles;
    private final List<? extends IBiLinkDef<?, T>> parents;

    public Entity(Class<T> clazz, Map<String, MultiLinkDef<T, ?>> multis, Map<String, SingleLinkDef<T, ?>> singles, List<? extends IBiLinkDef<?, T>> parents) {
      this.clazz = clazz;
      this.multis = multis;
      this.singles = singles;
      this.parents = parents;
    }

    public Collection<MultiLinkDef<T, ?>> multies() {
      return multis.values();
    }

    public Collection<SingleLinkDef<T, ?>> singles() {
      return singles.values();
    }

    public Object parents() {
      // TODO Auto-generated method stub
      return parents;
    }

  }

  public <T> List<DataWrap<T>> fetchAll(IPersister persister, Class<T> clazz) {
    List<T> nodes = persister.allOf(clazz);
    List<DataWrap<T>> pack = new ArrayList<DataWrap<T>>();
    for (T node : nodes) {
      DataWrap<T> wrap = createWrap(persister, node);
      pack.add(wrap);
    }
    return pack;
  }

  public <T> DataWrap<T> createWrap(IPersister persister, T node) {
    DataWrap.Builder<T> wrapBuilder = new DataWrap.Builder<T>(node);
    @SuppressWarnings("unchecked")
    Entity<T> entity = (Entity<T>) entities.get(PersisterUtil.kindOf(node.getClass()));
    if (entity != null) {
      for (MultiLinkDef<T, ?> multi : entity.multies()) {
        List<?> links = multi.links(persister, node);
        List<DataWrap<?>> wrapperLinks = new ArrayList<DataWrap<?>>();
        for (Object link : links)
          wrapperLinks.add(createWrap(persister, link));
        wrapBuilder.addList(multi.getKind(), wrapperLinks);
      }
      for (SingleLinkDef<T, ?> single : entity.singles()) {
        Object link = single.single(persister, node);
        DataWrap<?> childWrap = createWrap(persister, link);
        wrapBuilder.add(single.getKind(), childWrap);
      }
      for (IBiLinkDef<?, T> parent : entity.parents) {
        Object link = parent.back().single(persister, node);
        if (link != null) {
          DataWrap<?> childWrap = createWrap(persister, link);
          wrapBuilder.add(parent.back().getKind(), childWrap);
        }
      }
    }
    return wrapBuilder.build();
  }

  public <T> void deleteByKey(IPersister persister, Object key, Class<T> clazz) {

    Entity<?> entity = entities.get(PersisterUtil.kindOf(clazz));

    if (entity != null) {
      for (MultiLinkDef<?, ?> multi : entity.multies()) {
        List<?> linkedKeys = multi.linkKeys(persister, key);
        for (Object linkKey : linkedKeys) {
          multi.disconnectKeys(persister, key, linkKey);
          deleteByKey(persister, linkKey, multi.bClazz);
        }
      }
      for (SingleLinkDef<?, ?> single : entity.singles()) {
        Object linkKey = single.singleKey(persister, key);
        if (linkKey != null) {
          single.disconnectKeys(persister, key, linkKey);
          deleteByKey(persister, linkKey, single.bClazz);
        }
      }
      for (IBiLinkDef<?, ?> parent : entity.parents) {
        Object parentKey = parent.back().singleKey(persister, key);
        if (parentKey != null) {
          parent.disconnectKeys(persister, parentKey, key);
        }
      }
    }
    persister.session().delete(PersisterUtil.kindOf(clazz), key);
  }

  public static <T> List<DataWrap<T>> rawRetchAll(IPersister persister, Class<T> clazz) {
    List<T> nodes = persister.allOf(clazz);
    List<DataWrap<T>> pack = Lists.transform(nodes, new IFunction<T, DataWrap<T>>() {
      @Override
      public DataWrap<T> get(T node) {
        return new DataWrap<T>(node);
      }
    });
    return pack;
  }

  public <T> DataWrap<T> getByKey(IPersister persister, Object key, Class<T> clazz) {
    T node = persister.get(clazz, key);
    return createWrap(persister, node);
  }

}
