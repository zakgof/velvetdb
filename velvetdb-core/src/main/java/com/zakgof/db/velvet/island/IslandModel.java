package com.zakgof.db.velvet.island;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.*;

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
            private final Map<String, IContextSingleGetter<K, V, ?>> attrs = new HashMap<>();
            private Comparator<DataWrap<K, V>> sort = null;

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

            public <CV> FetcherEntityBuilder<K, V> attribute(String name, IContextSingleGetter<K, V, CV> contextSingleGetter) {
                attrs.put(name, contextSingleGetter);
                return this;
            }

            public FetcherEntityBuilder<K, V> sort(Comparator<V> comparator) {
                return sortWraps(Comparator.comparing(DataWrap::getNode, comparator));
            }

            public FetcherEntityBuilder<K, V> sortWraps(Comparator<DataWrap<K, V>> comparator) {
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
        private final Comparator<DataWrap<K, V>> sort;
        private final Map<String, IContextSingleGetter<K, V, ?>> attrs;

        private FetcherEntity(IEntityDef<K, V> entityDef,
                              Map<String, IMultiGetter<K,V,?,?>> multis,
                              Map<String, ISingleGetter<K,V,?,?>> singles,
                              Map<String, IContextSingleGetter<K, V, ?>> attrs,
                              Comparator<DataWrap<K, V>> sort) {
            this.entityDef = entityDef;
            this.multis = multis;
            this.singles = singles;
            this.attrs = attrs;
            this.sort = sort;
        }

    }

    public <K, V> DataWrap<K, V> get(IVelvet velvet, IEntityDef<K, V> entityDef, K key) {
        return wrap(velvet, entityDef, entityDef.get(velvet, key));
    }

    public <K, V> List<DataWrap<K, V>> getByKeys(IVelvet velvet, IEntityDef<K, V> entityDef, Collection<K> keys) {
        Stream<DataWrap<K, V>> stream = keys.stream().map(key -> get(velvet, entityDef, key));
        @SuppressWarnings("unchecked")
		FetcherEntity<K, V> fetcher = (FetcherEntity<K, V>) entities.get(entityDef);        
        Comparator<DataWrap<K, V>> comparator = (fetcher == null) ?  null : fetcher.sort;
        if (comparator != null) {
        	stream = stream.sorted(comparator);
        }
		return stream.collect(Collectors.toList());
    }

    public <K, V> List<DataWrap<K, V>> getAll(IVelvet velvet, IEntityDef<K, V> entityDef) {
        List<DataWrap<K, V>> wrap = entityDef.get(velvet).stream().map(node -> createWrap(velvet, entityDef, node, null)).collect(Collectors.toList());
        return wrap;
    }

    public <K, V> DataWrap<K, V> wrap(IVelvet velvet, IEntityDef<K, V> entityDef, V node) {
        return createWrap(velvet, entityDef, node, null);
    }

    private <K, V> DataWrap<K, V> createWrap(IVelvet velvet, IEntityDef<K, V> entityDef, V node, Context<?, ?> parentContext) {
        Context<K, V> context = new Context<>(parentContext, entityDef, node);
        DataWrap.Builder<K, V> wrapBuilder = new DataWrap.Builder<>(node);
        @SuppressWarnings("unchecked")
        FetcherEntity<K, V> entity = (FetcherEntity<K, V>) entities.get(entityDef);
        if (entity == null) {
            return wrapBuilder.build();
        }
        K key = entity.entityDef.keyOf(node);
        wrapBuilder.key(key);
        context.setKey(key);

        if (entity != null) {
            for (Entry<String, ? extends IMultiGetter<K, V, ?, ?>> entry : entity.multis.entrySet()) {
                IMultiGetter<K, V, ?, ?> multiLinkDef = entry.getValue();
                List<? extends DataWrap<?, ?>> wrappedLinks = wrapChildren(velvet, context, node, multiLinkDef);
                wrapBuilder.addList(entry.getKey(), wrappedLinks);
            }
            for (Entry<String, ? extends ISingleGetter<K, V, ?, ?>> entry : entity.singles.entrySet()) {
                ISingleGetter<K, V, ?, ?> singleConn = entry.getValue();
                DataWrap<?,?> wrappedLink = wrapChild(velvet, context, node, singleConn);
                if (wrappedLink != null)
                    wrapBuilder.add(entry.getKey(), wrappedLink);
            }
            for (Entry<String, IContextSingleGetter<K, V, ?>> entry : entity.attrs.entrySet()) {
                Object link = entry.getValue().single(velvet, context);
                if (link != null) {
                    wrapBuilder.attr(entry.getKey(), link);
                }
            }
        }
        DataWrap<K, V> wrap = wrapBuilder.build();
        context.addWrap(wrap);
        return wrap;
    }

    private <K, V, CK, CV> DataWrap<CK, CV> wrapChild(IVelvet velvet, Context<K, V> context, V node, ISingleGetter<K, V, CK, CV> singleGetter) {
        CV childValue = singleGetter.single(velvet, node);
        if (childValue == null)
            return null;
        return createWrap(velvet, singleGetter.getChildEntity(), childValue, context);
    }

    private <K, V, CK, CV> List<DataWrap<CK, CV>> wrapChildren(IVelvet velvet, Context<K, V> context, V node, IMultiGetter<K, V, CK, CV> multiGetter) {
        @SuppressWarnings("unchecked")
        FetcherEntity<CK, CV> childFetcher = (FetcherEntity<CK, CV>) entities.get(multiGetter.getChildEntity());
        Comparator<DataWrap<CK, CV>> comparator = (childFetcher == null) ?  null : childFetcher.sort;
        Stream<DataWrap<CK, CV>> stream = multiGetter.multi(velvet, node).stream()
            .map(o -> createWrap(velvet, multiGetter.getChildEntity(), o, context));
        if (comparator != null) {
            stream = stream.sorted(comparator);
        }
        return stream.collect(Collectors.toList());
    }

    public static <K, V> List<DataWrap<K, V>> rawRetchAll(IVelvet velvet, IEntityDef<K, V> entityDef) {
        List<DataWrap<K, V>> nodes = entityDef.get(velvet).stream().map(node -> new DataWrap<>(node, entityDef.keyOf(node))).collect(Collectors.toList());
        return nodes;
    }

    public interface IIslandContext<K, V> {

        V current();

        K currentKey();

        <CK, CV> CV node(IEntityDef<CK, CV> def);

        <CK, CV> DataWrap<CK, CV> wrap(IEntityDef<CK, CV> def);

    }

    // TODO: refactor, add parents and make immutable
    private static class Context<K, V> implements IIslandContext<K, V> {

        private final IEntityDef<K, V> entityDef;
        private final V current;
        private final Map<IEntityDef<?, ?>, Object> nodes = new HashMap<>();
        private final Map<IEntityDef<?, ?>, DataWrap<?, ?>> wraps = new HashMap<>();
        private K currentKey;

        private Context(Context<?, ?> parentContext, IEntityDef<K, V> entityDef, V node) {
            this.current = node;
            this.entityDef = entityDef;
            nodes.put(entityDef, node);
        }

        public void setKey(K key) {
            this.currentKey = key;
        }

        public void addWrap(DataWrap<K, V> wrap) {
            wraps.put(entityDef, wrap);
        }

        @Override
        public <CK, CV> CV node(IEntityDef<CK, CV> def) {
            return def.getValueClass().cast(nodes.get(def));
        }

        @SuppressWarnings("unchecked")
        @Override
        public <CK, CV> DataWrap<CK, CV> wrap(IEntityDef<CK, CV> def) {
            return (DataWrap<CK, CV>) wraps.get(def);
        }

        @Override
        public V current() {
            return current;
        }

        @Override
        public K currentKey() {
            return currentKey;
        }

    }

}
