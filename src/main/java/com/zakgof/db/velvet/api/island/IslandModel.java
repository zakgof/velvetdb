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
import com.zakgof.db.velvet.VelvetUtil;
import com.zakgof.db.velvet.api.link.IBiLinkDef;
import com.zakgof.db.velvet.api.link.IMultiLinkDef;
import com.zakgof.db.velvet.api.link.ISingleLinkDef;
import com.zakgof.db.velvet.api.link.Links;
import com.zakgof.tools.generic.Functions;
import com.zakgof.tools.generic.IFunction;

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
      private final Map<String, IMultiLinkDef<?, T, ?, ?>> multis = new HashMap<>();
      private final Map<Class<?>, IFunction<?, ? extends Comparable<?>>> sorts = new HashMap<>();
      private final Map<String, ISingleLinkDef<?, T, ?, ?>> singles = new HashMap<>();
      private final Map<String, IContextSingleGetter<?>> singleContexts = new HashMap<>();
      private final Map<String, IContextMultiGetter<?>> multiContexts = new HashMap<>();
      private final List<IBiLinkDef<?, T, ?, ?, ?>> detaches = new ArrayList<>();

      public FetcherEntityBuilder(Class<T> clazz) {
        this.clazz = clazz;
      }

      public <L, C extends Comparable<C>> FetcherEntityBuilder<T> include(IMultiLinkDef<?, T, ?, L> linkDef, IFunction<L, C> sortBy) {
        multis.put(linkDef.getKind(), linkDef);
        sorts.put(linkDef.getChildEntity().getValueClass(), sortBy);
        return this;
      }
      
      public <L> FetcherEntityBuilder<T> include(IMultiLinkDef<?, T, ?, L> linkDef) {
        multis.put(linkDef.getKind(), linkDef);        
        return this;
      }

      public <L> FetcherEntityBuilder<T> include(ISingleLinkDef<?, T, ?, L> linkDef) {
        singles.put(linkDef.getKind(), linkDef);
        return this;
      }

      public <P> FetcherEntityBuilder<T> detach(IBiLinkDef<?, T, ?, P, ?> out) {
        this.detaches.add(out);
        return this;
      }

      public <L> FetcherEntityBuilder<T> include(String name, IContextSingleGetter<L> contextSingleGetter) {
        singleContexts.put(name, contextSingleGetter);
        return this;
      }
      
      public <L> FetcherEntityBuilder<T> include(String name, IContextMultiGetter<L> contextMultiGetter) {
        multiContexts.put(name, contextMultiGetter);
        return this;
      }

      public Builder done() {
        Builder.this.addEntity(new FetcherEntity<T>(clazz, multis, singles, singleContexts, multiContexts, detaches, sorts));
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
    private final Map<String, IMultiLinkDef<?, T, ?, ?>> multis;
    private final Map<String, ISingleLinkDef<?, T, ?, ?>> singles;
    private final Map<String, IContextSingleGetter<?>> singleContexts;
    private final Map<String, IContextMultiGetter<?>> multiContexts;
    private final List<? extends IBiLinkDef<?, T, ?, ?, ?>> detaches;
    private final Map<Class<?>, IFunction<?, ? extends Comparable<?>>> sorts;
    

    private FetcherEntity(Class<T> clazz, Map<String, IMultiLinkDef<?, T, ?, ?>> multis, Map<String, ISingleLinkDef<?, T, ?, ?>> singles, Map<String, IContextSingleGetter<?>> singleContexts, Map<String, IContextMultiGetter<?>> multiContexts, List<? extends IBiLinkDef<?, T, ?, ?, ?>> detaches, Map<Class<?>, IFunction<?, ? extends Comparable<?>>> sorts) {
      this.clazz = clazz;
      this.multis = multis;
      this.singles = singles;
      this.singleContexts = singleContexts;
      this.multiContexts = multiContexts;
      this.detaches = detaches;
      this.sorts = sorts;
    }

  }

  public <T> List<DataWrap<T>> fetchAll(IVelvet velvet, Class<T> clazz) {
    List<DataWrap<T>> wrap = velvet.allOf(clazz).stream().map(node -> createWrap(velvet, node, new Context())).collect(Collectors.toList());
    return wrap;
  }
  
  public <T> DataWrap<T> createWrap(IVelvet velvet, T node) {
    return createWrap(velvet, node, new Context());
  }

  private <T> DataWrap<T> createWrap(IVelvet velvet, T node, Context context) {
    context.add(node);
    DataWrap.Builder<T> wrapBuilder = new DataWrap.Builder<T>(node);
    @SuppressWarnings("unchecked")
    FetcherEntity<T> entity = (FetcherEntity<T>) entities.get(VelvetUtil.kindOf(node.getClass()));
    if (entity != null) {
      for (Entry<String, ? extends IMultiLinkDef<?, T, ?, ?>> entry : entity.multis.entrySet()) {
        Stream<?> stream = entry.getValue().multi(velvet, node).stream().filter(l -> l != null);// TODO : check for error
        List<DataWrap<?>> wrappedLinks = decorateBySort(entity, stream, entry.getValue().getChildEntity().getValueClass()).map(o -> createWrap(velvet, o, context)).collect(Collectors.toList());
        wrapBuilder.addList(entry.getKey(), wrappedLinks);
      }
      for (Entry<String, IContextMultiGetter<?>> entry : entity.multiContexts.entrySet()) {
        Stream<?> stream = entry.getValue().multi(velvet, context);
        List<DataWrap<?>> wrappedLinks = stream.map(o -> createWrap(velvet, o, context)).collect(Collectors.toList());
        wrapBuilder.addList(entry.getKey(), wrappedLinks);
      }
      for (Entry<String, ? extends ISingleLinkDef<?, T, ?, ?>> entry : entity.singles.entrySet()) {
        Object link = entry.getValue().single(velvet, node);
        if (link != null) {
          DataWrap<?> childWrap = createWrap(velvet, link, context);
          wrapBuilder.add(entry.getKey(), childWrap);
        }
      }
      for (Entry<String, IContextSingleGetter<?>> entry : entity.singleContexts.entrySet()) {
        Object link = entry.getValue().single(velvet, (IIslandContext)context);
        if (link != null) {
          DataWrap<?> childWrap = createWrap(velvet, link, context);
          wrapBuilder.add(entry.getKey(), childWrap);
        }
      }
    }
    return wrapBuilder.build();
  }

  @SuppressWarnings("unchecked")  
  private <T> Stream<?> decorateBySort(FetcherEntity<T> entity, Stream<?> stream, Class<?> childClass) {
    IFunction<?, ? extends Comparable<?>> sorter = entity.sorts.get(childClass);
    return (sorter != null) ? stream.sorted(Functions.comparator((IFunction) sorter)) : stream;
  }

  // TODO: reimplement
  public <T> void deleteByKey(IVelvet velvet, Object key, Class<T> clazz) {    
    T node = velvet.get(clazz, key);
    deleteNode(velvet, node);
  }
  
  public <T> void deleteAll(IVelvet velvet, Class<T> clazz) {
    List<T> nodes = velvet.allOf(clazz);
    for (T node : nodes)
      deleteNode(velvet, node);
  }
  
  public <T> void deleteNode(IVelvet velvet, T node) {
    String kind = VelvetUtil.kindOf(node.getClass());
    @SuppressWarnings("unchecked")
    FetcherEntity<T> entity = (FetcherEntity<T>) entities.get(kind);
    if (entity != null) {
      for (IMultiLinkDef<?, T, ?, ?> multi : entity.multis.values())
        dropChildren(velvet, node, multi);
      for (ISingleLinkDef<?, T, ?, ?> single : entity.singles.values())
        dropChild(velvet, node, single);
      for (IBiLinkDef<?, T, ?, ?, ?> detach : entity.detaches)
        detach(velvet, node, detach);
    }
    velvet.delete(node);
  }

  private <T, B> void dropChild(IVelvet velvet, T node, ISingleLinkDef<?, T, ?, B> single) {
    B child = single.single(velvet, node);
    if (child != null) {
      single.disconnect(velvet, node, child);
      deleteNode(velvet, child);
    }
  }

  private <T, B> void dropChildren(IVelvet velvet, T node, IMultiLinkDef<?, T, ?, B> multi) {
    List<B> children = multi.multi(velvet, node);
    for (B child : children)  {
      multi.disconnect(velvet, node, child);
      deleteNode(velvet, child);
    }
  }

  private <T, A> void detach(IVelvet velvet, T node, IBiLinkDef<?, T, ?, A, ?> detach) {
    List<A> parents = Links.toMultiGetter(detach).multi(velvet, node);
    for (A parent : parents)
      detach.disconnect(velvet, node, parent);
  }

  public static <T> List<DataWrap<T>> rawRetchAll(IVelvet velvet, Class<T> clazz) {
    List<DataWrap<T>> nodes = velvet.allOf(clazz).stream().map(node -> new DataWrap<T>(node)).collect(Collectors.toList());
    return nodes;
  }

  public <T> DataWrap<T> getByKey(IVelvet velvet, Object key, Class<T> clazz) {
    T node = velvet.get(clazz, key);
    if (node == null)
      return null;
    return createWrap(velvet, node, new Context());
  }
  
  public <T> void save(IVelvet velvet, Collection<DataWrap<T>> data) {
    for (DataWrap<T> wrap : data)
      save(velvet, wrap);
  }
  
  public <T> void save(IVelvet velvet, DataWrap<T> data) {
    T node = data.getNode();   
    velvet.put(node);
    String kind = VelvetUtil.kindOf(node.getClass());
    @SuppressWarnings("unchecked")
    FetcherEntity<T> entity = (FetcherEntity<T>) entities.get(kind);    
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
    for (DataWrap<B> childWrap : childrenWraps)  {
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
