package com.zakgof.db.velvet.island;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
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
            private final Set<ISingleLinkDef<K,V,?,?>> detaches = new HashSet<>();
            private final Map<String, IMultiGetter<K,V,?,?>> multis = new HashMap<>();
            private final Map<String, IContextSingleGetter<K, V, ?>> attrs = new HashMap<>();
            private final Map<String, Function<DataWrap<K, V>, ?>> postattrs = new HashMap<>();
            private Comparator<DataWrap<K, V>> sort = null;

            public FetcherEntityBuilder(IEntityDef<K, V> entityDef) {
                this.entityDef = entityDef;
            }

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

            public <CK, CV> FetcherEntityBuilder<K, V> detach(ISingleLinkDef<K, V, CK, CV> parentLink) {
                detaches.add(parentLink);
                return this;
            }

            public <CV> FetcherEntityBuilder<K, V> attribute(String name, IContextSingleGetter<K, V, CV> contextSingleGetter) {
                attrs.put(name, contextSingleGetter);
                return this;
            }

            public <CV> FetcherEntityBuilder<K, V> attribute(String name, Function<DataWrap<K, V>, CV> function) {
                postattrs.put(name, function);
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
                Builder.this.addEntity(new FetcherEntity<>(entityDef, multis, singles, detaches, attrs, postattrs, sort));
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
        private final Set<ISingleLinkDef<K, V, ?, ?>> detaches;
        private final Comparator<DataWrap<K, V>> sort;
        private final Map<String, IContextSingleGetter<K, V, ?>> attrs;
        private final Map<String, Function<DataWrap<K, V>, ?>> postattrs;

        private FetcherEntity(IEntityDef<K, V> entityDef,
                              Map<String, IMultiGetter<K,V,?,?>> multis,
                              Map<String, ISingleGetter<K,V,?,?>> singles,
                              Set<ISingleLinkDef<K, V, ?, ?>> detaches,
                              Map<String, IContextSingleGetter<K, V, ?>> attrs,
                              Map<String, Function<DataWrap<K, V>, ?>> postattrs,
                              Comparator<DataWrap<K, V>> sort) {
            this.entityDef = entityDef;
            this.multis = multis;
            this.singles = singles;
            this.detaches = detaches;
            this.attrs = attrs;
            this.postattrs = postattrs;
            this.sort = sort;
        }

    }

    public <K, V> DataWrap<K, V> get(IVelvet velvet, IEntityDef<K, V> entityDef, K key) {
        V node = entityDef.get(velvet, key);
        DataWrap<K, V> wrap = new BatchBuilder<>(velvet, entityDef).make(node);
        return wrap;
    }

    public <K, V> List<DataWrap<K, V>> getByKeys(IVelvet velvet, IEntityDef<K, V> entityDef, List<K> keys) {
        List<V> nodes = entityDef.batchGetList(velvet, keys);
        return wrapNodes(velvet, entityDef, nodes);
    }

    public <K, V> List<DataWrap<K, V>> getAll(IVelvet velvet, IEntityDef<K, V> entityDef) {
        List<V> nodes = entityDef.batchGetAllList(velvet);
        return wrapNodes(velvet, entityDef, nodes);
    }

    public <K, V>  void delete(IVelvet velvet, IEntityDef<K, V> entityDef, K key) {
        @SuppressWarnings("unchecked")
        FetcherEntity<K, V> entity = (FetcherEntity<K, V>) entities.get(entityDef);
        if (entity != null) {
            for (Entry<String, IMultiGetter<K, V, ?, ?>> entry : entity.multis.entrySet()) {
                killChildren(velvet, entry.getValue(), key);
            }
            for (Entry<String, ISingleGetter<K, V, ?, ?>> entry : entity.singles.entrySet()) {
                killChild(velvet, entry.getValue(), key);
            }
            for (ISingleLinkDef<K, V, ?, ?> parent : entity.detaches) {
                detachParent(velvet, parent, key);
            }
        }
        entityDef.deleteKey(velvet, key);
    }

    public <K, V> DataWrap<K, V> wrap(IVelvet velvet, IEntityDef<K, V> entityDef, V node) {
        DataWrap<K, V> wrap = new BatchBuilder<>(velvet, entityDef).make(node);
        return wrap;
    }

    public <K, V> List<DataWrap<K, V>> wrapNodes(IVelvet velvet, IEntityDef<K, V> entityDef, List<V> nodes) {
        List<DataWrap<K, V>> wrapList = new BatchBuilder<>(velvet, entityDef).make(nodes);
        return wrapList;
    }

    private <K, V, CK, CV> void killChild(IVelvet velvet, ISingleGetter<K, V, CK, CV> singleGetter, K key) {
        if (singleGetter instanceof ISingleLinkDef<?, ?, ?, ?>) {
            ISingleLinkDef<K, V, CK, CV> singleLinkDef = (ISingleLinkDef<K, V, CK, CV>)singleGetter;
            CK childKey = singleLinkDef.key(velvet, key);
            if (childKey != null) {
                singleLinkDef.disconnectKeys(velvet, key, childKey);
                delete(velvet, singleLinkDef.getChildEntity(), childKey);
            }
        }
    }

    private <K, V, CK, CV> void killChildren(IVelvet velvet, IMultiGetter<K, V, CK, CV> multiGetter, K key) {
        if (multiGetter instanceof IMultiLinkDef<?, ?, ?, ?>) {
            IMultiLinkDef<K, V, CK, CV> multiLinkDef = (IMultiLinkDef<K, V, CK, CV>)multiGetter;
            List<CK> childKeys = multiLinkDef.keys(velvet, key);
            for (CK childKey : childKeys) {
                multiLinkDef.disconnectKeys(velvet, key, childKey);
            }
            for (CK childKey : childKeys) {
                delete(velvet, multiLinkDef.getChildEntity(), childKey);
            }
        }
    }

    private <K, V, CK, CV> void detachParent(IVelvet velvet, ISingleLinkDef<K, V, CK, CV> parentLinkDef, K key) {
        CK parentKey = parentLinkDef.key(velvet, key);
        if (parentKey != null) {
            parentLinkDef.disconnectKeys(velvet, key, parentKey);
        }
    }

    private <K, V> Stream<DataWrap<K, V>> sortTheseWraps(IEntityDef<K, V> entityDef, Stream<DataWrap<K, V>> stream) {
        @SuppressWarnings("unchecked")
        FetcherEntity<K, V> fetcher = (FetcherEntity<K, V>) entities.get(entityDef);
        Comparator<DataWrap<K, V>> comparator = (fetcher == null) ?  null : fetcher.sort;
        if (comparator != null) {
            stream = stream.sorted(comparator);
        }
        return stream;
    }

    /*

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
            DataWrap<K, V> wrap = wrapBuilder.build();
            wrapBuilder = new DataWrap.Builder<>(wrap);
            for (Entry<String, Function<DataWrap<K, V>, ?>> entry : entity.postattrs.entrySet()) {
                wrapBuilder.attr(entry.getKey(), entry.getValue().apply(wrap));
            }
        }
        DataWrap<K, V> wrap = wrapBuilder.build();
        context.addWrap(wrap);
        return wrap;
    }


    private <K, V, CK, CV> DataWrap<CK, CV> wrapChild(IVelvet velvet, Context<K, V> context, V node, ISingleGetter<K, V, CK, CV> singleGetter) {
        CV childValue = singleGetter.get(velvet, node);
        if (childValue == null)
            return null;
        return createWrap(velvet, singleGetter.getChildEntity(), childValue, context);
    }

    private <K, V, CK, CV> List<DataWrap<CK, CV>> wrapChildren(IVelvet velvet, Context<K, V> context, V node, IMultiGetter<K, V, CK, CV> multiGetter) {
        @SuppressWarnings("unchecked")
        FetcherEntity<CK, CV> childFetcher = (FetcherEntity<CK, CV>) entities.get(multiGetter.getChildEntity());
        Comparator<DataWrap<CK, CV>> comparator = (childFetcher == null) ?  null : childFetcher.sort;
        Stream<DataWrap<CK, CV>> stream = multiGetter.get(velvet, node).stream()
            .map(o -> createWrap(velvet, multiGetter.getChildEntity(), o, context));
        if (comparator != null) {
            stream = stream.sorted(comparator);
        }
        return stream.collect(Collectors.toList());
    }
    */

    public static <K, V> List<DataWrap<K, V>> rawRetchAll(IVelvet velvet, IEntityDef<K, V> entityDef) {
        List<DataWrap<K, V>> nodes = entityDef.batchGetAllList(velvet).stream().map(node -> new DataWrap<>(node, entityDef.keyOf(node))).collect(Collectors.toList());
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

    private class BatchBuilder<KK, VV> {

        private IVelvet velvet;
        private IEntityDef<KK, VV> startEntity;

        private Map<ISingleGetter<?,?,?,?>, Map<?, ?>> singles = new HashMap<>();
        private Map<IMultiGetter<?,?,?,?>, Map<?, List<?>>> multis = new HashMap<>();

        public BatchBuilder(IVelvet velvet, IEntityDef<KK, VV> startEntity) {
            this.velvet = velvet;
            this.startEntity = startEntity;
        }

        public DataWrap<KK, VV> make(VV node) {
            preFetch(startEntity, Arrays.asList(node));
            DataWrap<KK, VV> wrap = wrap(startEntity, node, null);
            return wrap;
        }

        private List<DataWrap<KK, VV>> make(List<VV> startNodes) {
            preFetch(startEntity, startNodes);
            return startNodes.stream()
                .map(node -> wrap(startEntity, node, null))
                .collect(Collectors.toList());
        }

        private <K, V> void preFetch(IEntityDef<K, V> entity, List<V> nodes) {
            @SuppressWarnings("unchecked")
            FetcherEntity<K, V> fentity = (FetcherEntity<K, V>) entities.get(entity);
            if (fentity == null) {
                return;
            }

            for (Entry<String, ? extends IMultiGetter<K, V, ?, ?>> entry : fentity.multis.entrySet()) {
                IMultiGetter<K, V, ?, ?> multi = entry.getValue();
                preFetchMulti(multi, nodes);
            }
            for (Entry<String, ? extends ISingleGetter<K, V, ?, ?>> entry : fentity.singles.entrySet()) {
                ISingleGetter<K, V, ?, ?> single = entry.getValue();
                preFetchSingle(single, nodes);
            }
        }

        private <K, V, CK, CV> void preFetchSingle(ISingleGetter<K, V, CK, CV> single, List<V> nodes) {
            Map<K, CV> children = single.batchGet(velvet, nodes);
            singles.computeIfAbsent(single, s -> new LinkedHashMap<>()).putAll((Map)children);
            List<CV> childnodes = children.values().stream().collect(Collectors.toList());
            preFetch(single.getChildEntity(), childnodes);
        }

        private <K, V, CK, CV> void preFetchMulti(IMultiGetter<K, V, CK, CV> multi, List<V> nodes) {
            Map<K, List<CV>> children = multi.batchGet(velvet, nodes);
            multis.computeIfAbsent(multi, m -> new LinkedHashMap<>()).putAll((Map)children);
            List<CV> childnodes = children.values().stream().flatMap(List::stream).collect(Collectors.toList());
            preFetch(multi.getChildEntity(), childnodes);
        }

        private <K, V> DataWrap<K, V> wrap(IEntityDef<K, V> entityDef, V node, Context<?, ?> parentContext) {
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
                    List<? extends DataWrap<?, ?>> wrappedLinks = wrapChildren(context, key, multiLinkDef);
                    wrapBuilder.addList(entry.getKey(), wrappedLinks);
                }
                for (Entry<String, ? extends ISingleGetter<K, V, ?, ?>> entry : entity.singles.entrySet()) {
                    ISingleGetter<K, V, ?, ?> singleConn = entry.getValue();
                    DataWrap<?,?> wrappedLink = wrapChild(context, key, singleConn);
                    if (wrappedLink != null)
                        wrapBuilder.add(entry.getKey(), wrappedLink);
                }
                for (Entry<String, IContextSingleGetter<K, V, ?>> entry : entity.attrs.entrySet()) {
                    Object link = entry.getValue().single(velvet, context);
                    if (link != null) {
                        wrapBuilder.attr(entry.getKey(), link);
                    }
                }
                DataWrap<K, V> wrap = wrapBuilder.build();
                wrapBuilder = new DataWrap.Builder<>(wrap);
                for (Entry<String, Function<DataWrap<K, V>, ?>> entry : entity.postattrs.entrySet()) {
                    wrapBuilder.attr(entry.getKey(), entry.getValue().apply(wrap));
                }
            }
            DataWrap<K, V> wrap = wrapBuilder.build();
            context.addWrap(wrap);
            return wrap;
        }

        private <K, V, CK, CV> DataWrap<CK, CV> wrapChild(Context<K, V> context, K key, ISingleGetter<K, V, CK, CV> single) {
            @SuppressWarnings("unchecked")
            CV childValue = (CV) singles.get(single).get(key);
            if (childValue == null)
                return null;
            return wrap(single.getChildEntity(), childValue, context);
        }

        private <K, V, CK, CV> List<DataWrap<CK, CV>> wrapChildren(Context<K, V> context, K key, IMultiGetter<K, V, CK, CV> multi) {
            @SuppressWarnings("unchecked")
            FetcherEntity<CK, CV> childFetcher = (FetcherEntity<CK, CV>) entities.get(multi.getChildEntity());
            Comparator<DataWrap<CK, CV>> comparator = (childFetcher == null) ?  null : childFetcher.sort;
            @SuppressWarnings("unchecked")
            List<CV> cvs = (List<CV>) multis.get(multi).get(key);
            Stream<DataWrap<CK, CV>> stream = cvs.stream()
                .map(o -> wrap(multi.getChildEntity(), o, context));
            if (comparator != null) {
                stream = stream.sorted(comparator);
            }
            return stream.collect(Collectors.toList());
        }

    }

}
