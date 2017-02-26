package com.zakgof.db.velvet.island;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.*;
import com.zakgof.db.velvet.query.IRangeQuery;

public class IslandModel {

  private final Map<String, FetcherEntity<?, ?>> entities;

  private IslandModel(Map<String, FetcherEntity<?, ?>> entities) {
    this.entities = entities;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final Map<String, FetcherEntity<?, ?>> entities = new LinkedHashMap<String, FetcherEntity<?, ?>>();

    public <K, V> FetcherEntityBuilder<K, V> entity(IEntityDef<K, V> entityDef) {
      return new FetcherEntityBuilder<K, V>(entityDef);
    }

    public class FetcherEntityBuilder<K, V> {

      private final IEntityDef<K, V> entityDef;
      
      private final Map<Class<?>, Comparator<?>> sorts = new HashMap<>();
      private final Map<String, ISingleConnector<K, V, ?, ?>> singles = new HashMap<>();
      private final Map<String, IMultiConnector<K, V, ?, ?>> multis = new HashMap<>();
      private final Map<String, IContextSingleGetter<?>> singleContexts = new HashMap<>();
      private final Map<String, IContextMultiGetter<?>> multiContexts = new HashMap<>();
      private final List<IBiLinkDef<K, V, ?, ?, ?>> detaches = new ArrayList<>();

      public FetcherEntityBuilder(IEntityDef<K, V> entityDef) {
        this.entityDef = entityDef;
      }

      public <L, C extends Comparable<C>> FetcherEntityBuilder<K, V> include(IMultiLinkDef<K, V, ?, L> linkDef, Function<L, C> sortBy) {
        multis.put(linkDef.getKind(), new MultiConnector<>(linkDef));
        sorts.put(linkDef.getChildEntity().getValueClass(), Comparator.<DataWrap<L>, C>comparing(w -> sortBy.apply(w.getNode())));
        return this;
      }

      public <L> FetcherEntityBuilder<K, V> include(IMultiLinkDef<K, V, ?, L> linkDef) {
        multis.put(linkDef.getKind(), new MultiConnector<>(linkDef));
        return this;
      }
      
      public <CK, CV, M extends Comparable<? super M>> FetcherEntityBuilder<K, V> include(ISortedMultiLink<K, V, CK, CV, M> indexed, IRangeQuery<CK, M> query) {
          multis.put(indexed.getKind(), new MultiGetterConnector<K, V, CK, CV>(indexed.indexed(query)));
          return this;
        }

      public <L> FetcherEntityBuilder<K, V> include(ISingleLinkDef<K, V, ?, L> linkDef) {
        singles.put(linkDef.getKind(), new SingleConnector<>(linkDef));
        return this;
      }


      public <P> FetcherEntityBuilder<K, V> detach(IBiLinkDef<K, V, ?, P, ?> out) {
        this.detaches.add(out);
        return this;
      }

      public <L> FetcherEntityBuilder<K, V> attr(String name, IContextSingleGetter<L> contextSingleGetter) {
        singleContexts.put(name, contextSingleGetter);
        return this;
      }
      
      public FetcherEntityBuilder<K, V> sort(Comparator<V> comparator) {
          return sortWraps(Comparator.comparing(DataWrap::getNode, comparator));
      }

      public FetcherEntityBuilder<K, V> sortWraps(Comparator<DataWrap<V>> comparator) {
          sorts.put(this.entityDef.getValueClass(), comparator);
          return this;
      }
      
      
//
//      public <L> FetcherEntityBuilder<K, V> withCollection(String name, IContextMultiGetter<L> contextMultiGetter) {
//        multiContexts.put(name, contextMultiGetter);
//        return this;
//      }
      
      public <L> FetcherEntityBuilder<K, V> include(String name, ISingleGetter<K, V, ?, L> getter) {
    	  singles.put(name, new SingleGetterConnector<>(getter));
	      return this;
	  }

    public Builder done() {
        Builder.this.addEntity(new FetcherEntity<K, V>(entityDef, multis, singles, multiContexts, singleContexts, detaches, sorts));
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
    private final Map<String, IMultiConnector<K, V, ?, ?>> multis;
    private final Map<String, ISingleConnector<K, V, ?, ?>> singles;
    private final List<? extends IBiLinkDef<K, V, ?, ?, ?>> detaches;
    private final Map<Class<?>, Comparator<?>> sorts;
	private final Map<String, IContextMultiGetter<?>> multiContexts;
	private final Map<String, IContextSingleGetter<?>> singleContexts;

    private FetcherEntity(IEntityDef<K, V> entityDef, Map<String, IMultiConnector<K, V, ?, ?>> multis, Map<String, ISingleConnector<K, V, ?, ?>> singles, Map<String, IContextMultiGetter<?>> multiContexts,
    		Map<String, IContextSingleGetter<?>> singleContexts, List<? extends IBiLinkDef<K, V, ?, ?, ?>> detaches, Map<Class<?>, Comparator<?>> sorts) {
      this.entityDef = entityDef;
      this.multis = multis;
      this.singles = singles;
      this.detaches = detaches;
      this.sorts = sorts;
      this.multiContexts = multiContexts; 
      this.singleContexts = singleContexts;
    }

  }

  public <T> List<DataWrap<T>> fetchAll(IVelvet velvet, IEntityDef<?, T> entityDef) {
    List<DataWrap<T>> wrap = entityDef.get(velvet).stream().map(node -> this.<T> createWrap(velvet, entityDef.getKind(), node, new Context())).collect(Collectors.toList());
    return wrap;
  }

	  public <T> DataWrap<T> createWrap(IVelvet velvet, IEntityDef<?, T> entityDef, T node) {
	    return createWrap(velvet, entityDef.getKind(), node, new Context());
	  }

//  @SuppressWarnings("unchecked")
//  private <T> IEntityDef<?, T> entityOf(T node) {
//    return Entities.anno((Class<T>) node.getClass()); // TODO sorted annos !
//  }

  private <T> DataWrap<T> createWrap(IVelvet velvet, String kind, T node, Context context) {
    context.add(node);
    DataWrap.Builder<T> wrapBuilder = new DataWrap.Builder<T>(node);
    @SuppressWarnings("unchecked")
    FetcherEntity<?, T> entity = (FetcherEntity<?, T>) entities.get(kind);
    if (entity == null) {
    	return wrapBuilder.build();
    }
    Object key = entity.entityDef.keyOf(node);
    wrapBuilder.key(key);
    
    if (entity != null) {
      for (Entry<String, ? extends IMultiConnector<?, T, ?, ?>> entry : entity.multis.entrySet()) {
    	  IMultiConnector<?, T, ?, ?> multiLinkDef = entry.getValue();        
        List<DataWrap<?>> wrappedLinks = wrapChildren(velvet, context, entity, node, multiLinkDef);
        wrapBuilder.addList(entry.getKey(), wrappedLinks);
      }
      for (Entry<String, IContextMultiGetter<?>> entry : entity.multiContexts.entrySet()) {
        IContextMultiGetter<?> getter = entry.getValue();
        Stream<?> stream = getter.multi(velvet, context);
        List<DataWrap<?>> wrappedLinks = stream.map(o -> createWrap(velvet, kindOf(o), o, context)).collect(Collectors.toList());
        wrapBuilder.addList(entry.getKey(), wrappedLinks);
      }
      for (Entry<String, ? extends ISingleConnector<?, T, ?, ?>> entry : entity.singles.entrySet()) {
    	  ISingleConnector<?, T, ? , ?> singleConn = entry.getValue();
        DataWrap<?> wrappedLink = wrapChild(velvet, context, node, singleConn);
        if (wrappedLink != null)
          wrapBuilder.add(entry.getKey(), wrappedLink);
      }
      for (Entry<String, IContextSingleGetter<?>> entry : entity.singleContexts.entrySet()) {
        Object link = entry.getValue().single(velvet, (IIslandContext) context);
        if (link != null) {
          DataWrap<?> childWrap = createWrap(velvet, kindOf(link), link, context);
          wrapBuilder.add(entry.getKey(), childWrap);
        }
      }
    }
    return wrapBuilder.build();
  }
  
  private String kindOf(Object o) {
	// TODO Auto-generated method stub
	return null;
}

private <T> DataWrap<?> wrapChild(IVelvet velvet, Context context, T node, ISingleConnector<?, T, ?, ?> singleConn) {
    Object childValue  = singleConn.getter().single(velvet, node);
    if (childValue == null)
      return null;
    return createWrap(velvet, singleConn.getter().getChildEntity().getKind(), childValue, context);
  }

  private <T, CK, CV> List<DataWrap<?>> wrapChildren(IVelvet velvet, Context context, FetcherEntity<?, T> entity, T node, IMultiConnector<?, T, CK, CV> multiConn) {
    Stream<DataWrap<CV>> stream = multiConn.getter().multi(velvet, node).stream()
       .filter(l -> l != null) // TODO : check for error
       .map(o -> createWrap(velvet, multiConn.getter().getChildEntity().getKind(), o, context));
       
       return decorateBySort(stream, entity, multiConn.getter().getChildEntity().getValueClass())
       .collect(Collectors.toList());
  }

  // Natural sorting
  @SuppressWarnings({"unchecked"})
  private <T, V> Stream<DataWrap<V>> decorateBySort(Stream<DataWrap<V>> stream, FetcherEntity<?, T> entity, Class<?> childClass) {
    Comparator<DataWrap<V>> sorter = (Comparator<DataWrap<V>>) entity.sorts.get(childClass);
    return (sorter != null) ? stream.sorted(sorter) : stream;
  }

  public <K, V> void deleteByKey(IVelvet velvet, K key, IEntityDef<K, V> entityDef) {
    String kind = entityDef.getKind();
    @SuppressWarnings("unchecked")
    FetcherEntity<K, V> entity = (FetcherEntity<K, V>) entities.get(kind);
    if (entity != null) {
      for (IMultiConnector<K, V, ?, ?> multi : entity.multis.values())
        dropChildren(velvet, key, multi);
      for (ISingleConnector<K, V, ?, ?> single : entity.singles.values())
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

  private <HK, HV, CK, CV> void dropChild(IVelvet velvet, HK key, ISingleConnector<HK, HV, CK, CV> singleConn) {
    CK childKey = singleConn.getter().singleKey(velvet, key);
    if (childKey != null) {
    	singleConn.disconnectKeys(velvet, key, childKey);
        deleteByKey(velvet, childKey, singleConn.getter().getChildEntity());
    }
  }

  private <HK, HV, CK, CV> void dropChildren(IVelvet velvet, HK key, IMultiConnector<HK, HV, CK, CV> multi) {
    List<CK> childrenKeys = multi.getter().multiKeys(velvet, key);
    for (CK childKey : childrenKeys) {
      multi.disconnectKeys(velvet, key, childKey);
      deleteByKey(velvet, childKey, multi.getter().getChildEntity());
    }
  }

  private <HK, HV, CK, CV> void detach(IVelvet velvet, HK key, IBiLinkDef<HK, HV, CK, CV, ?> detach) {
    List<CK> parentKeys = Links.toMultiGetter(detach).multiKeys(velvet, key);
    for (CK parentKey : parentKeys)
      detach.disconnectKeys(velvet, key, parentKey);
  }

  public static <K, V> List<DataWrap<V>> rawRetchAll(IVelvet velvet, IEntityDef<K, V> entityDef) {
    List<DataWrap<V>> nodes = entityDef.get(velvet).stream().map(node -> new DataWrap<V>(node, entityDef.keyOf(node))).collect(Collectors.toList());
    return nodes;
  }

  public <K, V> DataWrap<V> getByKey(IVelvet velvet, K key, IEntityDef<K, V> entityDef) {
    V node = entityDef.get(velvet, key);
    if (node == null)
      return null;
    return createWrap(velvet, entityDef.getKind(), node, new Context());
  }
  
  public <K, V> List<DataWrap<V>> getByKeys(IVelvet velvet, Collection<K> keys, IEntityDef<K, V> entityDef) {
	return 
	    decorateBySort(    
    	    keys.stream()
            .map(key -> getByKey(velvet, key, entityDef)), entities.get(entityDef.getKind()), entityDef.getValueClass())
        .collect(Collectors.toList());
  }

  /*
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
  */
  
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
  
  private interface ISingleConnector<HK, HV, CK, CV> {
	  ISingleGetter<HK, HV, CK, CV> getter();
	  void disconnectKeys(IVelvet velvet, HK key, CK childKey);
  }
  
  private interface IMultiConnector<HK, HV, CK, CV> {
	  IMultiGetter<HK, HV, CK, CV> getter();
	  void disconnectKeys(IVelvet velvet, HK key, CK childKey);
  }
  
  private static class SingleConnector<HK, HV, CK, CV> implements ISingleConnector<HK, HV, CK, CV> {

		private ISingleLinkDef<HK, HV, CK, CV> link;

		public SingleConnector(ISingleLinkDef<HK, HV, CK, CV> link) {
			this.link = link;
		}

		public ISingleGetter<HK, HV, CK, CV> getter() {
			return link;
		}

		public void disconnectKeys(IVelvet velvet, HK key, CK childKey) {
			link.disconnectKeys(velvet, key, childKey);
		}
	}
  
	private static class MultiConnector<HK, HV, CK, CV> implements IMultiConnector<HK, HV, CK, CV> {

		private IMultiLinkDef<HK, HV, CK, CV> link;

		public MultiConnector(IMultiLinkDef<HK, HV, CK, CV> link) {
			this.link = link;
		}

		public IMultiGetter<HK, HV, CK, CV> getter() {
			return link;
		}

		public void disconnectKeys(IVelvet velvet, HK key, CK childKey) {
			link.disconnectKeys(velvet, key, childKey);
		}
	}
	
	private static class MultiGetterConnector<HK, HV, CK, CV> implements IMultiConnector<HK, HV, CK, CV> {

		private IMultiGetter<HK, HV, CK, CV> getter;

		public MultiGetterConnector(IMultiGetter<HK, HV, CK, CV> getter) {
			this.getter = getter;
		}

		public IMultiGetter<HK, HV, CK, CV> getter() {
			return getter;
		}

		public void disconnectKeys(IVelvet velvet, HK key, CK childKey) {
			throw new IllegalArgumentException();
		}
	}
	
	private static class SingleGetterConnector<HK, HV, CK, CV> implements ISingleConnector<HK, HV, CK, CV> {

		private ISingleGetter<HK, HV, CK, CV> getter;

		public SingleGetterConnector(ISingleGetter<HK, HV, CK, CV> getter) {
			this.getter = getter;
		}

		public ISingleGetter<HK, HV, CK, CV> getter() {
			return getter;
		}

		public void disconnectKeys(IVelvet velvet, HK key, CK childKey) {
			throw new IllegalArgumentException();
		}
	}

	public <K, V> void deleteNode(IVelvet velvet, V value) {
		// TODO this is crazy
		@SuppressWarnings("unchecked")
		IEntityDef<K, V> entity = (IEntityDef<K, V>) entities.entrySet().iterator().next().getValue().entityDef;
		deleteByKey(velvet, entity.keyOf(value), entity);
	}
  
  

}
