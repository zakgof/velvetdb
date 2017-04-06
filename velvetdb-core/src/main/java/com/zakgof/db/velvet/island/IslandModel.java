package com.zakgof.db.velvet.island;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IMultiGetter;
import com.zakgof.db.velvet.link.IMultiLinkDef;
import com.zakgof.db.velvet.link.ISingleGetter;
import com.zakgof.db.velvet.link.ISingleLinkDef;

public class IslandModel {

    private final Map<IEntityDef<?, ?>, FetcherEntity<?, ?>> entities;

    private IslandModel(Map<IEntityDef<?, ?>, FetcherEntity<?, ?>> entities) {
        this.entities = entities;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final Map<IEntityDef<?, ?>, FetcherEntity<?, ?>> entities = new LinkedHashMap<>();

        public <K, V> FetcherEntityBuilder<K, V> entity(IEntityDef<K, V> entityDef) {
            return new FetcherEntityBuilder<>(entityDef);
        }

        public class FetcherEntityBuilder<K, V> {

            private final IEntityDef<K, V> entityDef;
            private final Map<String, ISingleGetter<K,V,?,?>> singles = new HashMap<>();
            private final Map<String, IMultiGetter<K,V,?,?>> multis = new HashMap<>();
            private final Map<String, IContextSingleGetter<?>> attrs = new HashMap<>();
            private Comparator<DataWrap<V>> sort = null;

            public FetcherEntityBuilder(IEntityDef<K, V> entityDef) {
                this.entityDef = entityDef;
            }

            // TODO: separate hierarchy for delete.

            public <CK, CV> FetcherEntityBuilder<K, V> include(String name, IMultiGetter<K, V, CK, CV> multigetter) {
                multis.put(name, multigetter);
                return this;
            }

            public <CK, CV> FetcherEntityBuilder<K, V> include(IMultiLinkDef<K, V, CK, CV> linkDef) {
                return include(linkDef.getKind(), linkDef);
            }

            public <CK, CV> FetcherEntityBuilder<K, V> include(String name, ISingleGetter<K, V, CK, CV> getter) {
                singles.put(name, getter);
                return this;
            }

            public <L> FetcherEntityBuilder<K, V> include(ISingleLinkDef<K, V, ?, L> linkDef) {
                return include(linkDef.getKind(), linkDef);
            }

            public <CK, CV> FetcherEntityBuilder<K, V> attribute(String name, IContextSingleGetter<CV> contextSingleGetter) {
                attrs.put(name, contextSingleGetter);
                return this;
            }

            public FetcherEntityBuilder<K, V> sort(Comparator<V> comparator) {
                return sortWraps(Comparator.comparing(DataWrap::getNode, comparator));
            }

            public FetcherEntityBuilder<K, V> sortWraps(Comparator<DataWrap<V>> comparator) {
                sort = comparator;
                return this;
            }

            public Builder done() {
                Builder.this.addEntity(new FetcherEntity<>(entityDef, multis, singles, attrs, sort));
                return Builder.this;
            }

        }

        private <K, V> void addEntity(FetcherEntity<K, V> entity) {
            entities.put(entity.entityDef, entity);
        }

        public IslandModel build() {
            return new IslandModel(entities);
        }

    }

    private static class FetcherEntity<K, V> {

        private final IEntityDef<K, V> entityDef;
        private final Map<String, IMultiGetter<K, V, ?, ?>> multis;
        private final Map<String, ISingleGetter<K, V, ?, ?>> singles;
        private final Comparator<DataWrap<V>> sort;
        private final Map<String, IContextSingleGetter<?>> attrs;

        private FetcherEntity(IEntityDef<K, V> entityDef,
                              Map<String, IMultiGetter<K,V,?,?>> multis,
                              Map<String, ISingleGetter<K,V,?,?>> singles,
                              Map<String, IContextSingleGetter<?>> attrs,
                              Comparator<DataWrap<V>> sort) {
            this.entityDef = entityDef;
            this.multis = multis;
            this.singles = singles;
            this.attrs = attrs;
            this.sort = sort;
        }

    }

    public <K, V> DataWrap<V> get(IVelvet velvet, IEntityDef<K, V> entityDef, K key) {
        return wrap(velvet, entityDef, entityDef.get(velvet, key));
    }

    public <K, V> List<DataWrap<V>> getByKeys(IVelvet velvet, IEntityDef<K, V> entityDef, Collection<K> keys) {
        return keys.stream().map(key -> get(velvet, entityDef, key)).collect(Collectors.toList());
    }

    public <K, V> List<DataWrap<V>> getAll(IVelvet velvet, IEntityDef<K, V> entityDef) {
        List<DataWrap<V>> wrap = entityDef.get(velvet).stream().map(node -> this.<V> createWrap(velvet, entityDef, node, new Context())).collect(Collectors.toList());
        return wrap;
    }

    public <V> DataWrap<V> wrap(IVelvet velvet, IEntityDef<?, V> entityDef, V node) {
        return createWrap(velvet, entityDef, node, new Context());
    }

    private <T> DataWrap<T> createWrap(IVelvet velvet, IEntityDef<?, T> entityDef, T node, Context context) {
        context.setCurrent(node);
        context.add(node);
        DataWrap.Builder<T> wrapBuilder = new DataWrap.Builder<>(node);
        @SuppressWarnings("unchecked")
        FetcherEntity<?, T> entity = (FetcherEntity<?, T>) entities.get(entityDef);
        if (entity == null) {
            return wrapBuilder.build();
        }
        Object key = entity.entityDef.keyOf(node);
        wrapBuilder.key(key);

        if (entity != null) {
            for (Entry<String, ? extends IMultiGetter<?,T,?,?>> entry : entity.multis.entrySet()) {
                IMultiGetter<?,T,?,?> multiLinkDef = entry.getValue();
                List<DataWrap<?>> wrappedLinks = wrapChildren(velvet, context, entity, node, multiLinkDef);
                wrapBuilder.addList(entry.getKey(), wrappedLinks);
            }
            for (Entry<String, ? extends ISingleGetter<?, T, ?, ?>> entry : entity.singles.entrySet()) {
                ISingleGetter<?, T, ?, ?> singleConn = entry.getValue();
                DataWrap<?> wrappedLink = wrapChild(velvet, context, node, singleConn);
                if (wrappedLink != null)
                    wrapBuilder.add(entry.getKey(), wrappedLink);
            }
            for (Entry<String, IContextSingleGetter<?>> entry : entity.attrs.entrySet()) {
                Object link = entry.getValue().single(velvet, context);
                if (link != null) {
                    wrapBuilder.attr(entry.getKey(), link);
                }
            }
        }
        DataWrap<T> wrap = wrapBuilder.build();
        context.addWrap(wrap);
        return wrap;
    }

    private <T, CK, CV> DataWrap<?> wrapChild(IVelvet velvet, Context context, T node, ISingleGetter<?, T, CK, CV> singleGetter) {
        CV childValue = singleGetter.single(velvet, node);
        if (childValue == null)
            return null;
        return createWrap(velvet, singleGetter.getChildEntity(), childValue, context);
    }

    private <T, CK, CV> List<DataWrap<?>> wrapChildren(IVelvet velvet, Context context, FetcherEntity<?, T> entity, T node, IMultiGetter<?, T, CK, CV> multiGetter) {
        @SuppressWarnings("unchecked")
        FetcherEntity<CK, CV> childFetcher = (FetcherEntity<CK, CV>) entities.get(multiGetter.getChildEntity());
        Comparator<DataWrap<CV>> comparator = (childFetcher == null) ?  null : childFetcher.sort;
        Stream<DataWrap<CV>> stream = multiGetter.multi(velvet, node).stream()
            .map(o -> createWrap(velvet, multiGetter.getChildEntity(), o, context));
        if (comparator != null) {
            stream = stream.sorted(comparator);
        }
        return stream.collect(Collectors.toList());
    }

    public static <K, V> List<DataWrap<V>> rawRetchAll(IVelvet velvet, IEntityDef<K, V> entityDef) {
        List<DataWrap<V>> nodes = entityDef.get(velvet).stream().map(node -> new DataWrap<>(node, entityDef.keyOf(node))).collect(Collectors.toList());
        return nodes;
    }

    /*
     * public <T> void save(IVelvet velvet, Collection<DataWrap<T>> data) { for (DataWrap<T> wrap : data) save(velvet, wrap); }
     *
     * public <T> void save(IVelvet velvet, DataWrap<T> data) { T node = data.getNode(); IEntityDef<?, T> entityDef = entityOf(node); entityDef.put(velvet, node); String kind = entityDef.getKind();
     *
     * @SuppressWarnings("unchecked") FetcherEntity<?, T> entity = (FetcherEntity<?, T>) entities.get(kind); if (entity != null) { for (ISingleLinkDef<?, T, ?, ?> single : entity.singles.values()) saveChild(velvet, data, single); for
     * (IMultiLinkDef<?, T, ?, ?> multi : entity.multis.values()) saveChildren(velvet, data, multi); } }
     *
     * private <T, B> void saveChild(IVelvet velvet, DataWrap<T> parentWrap, ISingleLinkDef<?, T, ?, B> singleLink) { DataWrap<B> childWrap = parentWrap.singleLink(singleLink); singleLink.connect(velvet, parentWrap.getNode(), childWrap.getNode());
     * save(velvet, childWrap); }
     *
     * private <T, B> void saveChildren(IVelvet velvet, DataWrap<T> parentWrap, IMultiLinkDef<?, T, ?, B> multiLink) { List<DataWrap<B>> childrenWraps = parentWrap.multiLink(multiLink); for (DataWrap<B> childWrap : childrenWraps) {
     * multiLink.connect(velvet, parentWrap.getNode(), childWrap.getNode()); save(velvet, childWrap); } }
     */

    public interface IIslandContext {

        public <T> T current();

        public <T> T get(Class<T> clazz);

        public <T> DataWrap<T> wrap(Class<T> clazz);
    }

    private static class Context implements IIslandContext {

        private Object current;
        private final Map<Class<?>, Object> map = new HashMap<>();
        private final Map<Class<?>, DataWrap<?>> wraps = new HashMap<>();

        public void setCurrent(Object node) {
            this.current = node;
        }

        public void add(Object node) {
            map.put(node.getClass(), node);
        }

        public <T> void addWrap(DataWrap<T> wrap) {
            wraps.put(wrap.getNode().getClass(), wrap);
        }

        @Override
        public <T> T get(Class<T> clazz) {
            return clazz.cast(map.get(clazz));
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> DataWrap<T> wrap(Class<T> clazz) {
            return (DataWrap<T>) wraps.get(clazz);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T current() {
            return (T) current;
        }

    }

}
