package com.zakgof.db.velvet.join;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IBiManyToManyLinkDef;
import com.zakgof.db.velvet.link.ILinkDef;
import com.zakgof.db.velvet.link.IMultiGetter;
import com.zakgof.db.velvet.link.IMultiLinkDef;
import com.zakgof.db.velvet.link.ISingleGetter;
import com.zakgof.db.velvet.link.ISingleLinkDef;
import com.zakgof.db.velvet.link.Links;

/**
 * Query for multiple linked entities.
 *
 * @param <MK> main entity key type
 * @param <MV> main entity value type
 */
public class JoinDef<MK, MV> {

    private final Map<IEntityDef<?, ?>, FetcherEntity<?, ?>> entities;
    private final IEntityDef<MK, MV> mainEntity;

    private JoinDef(IEntityDef<MK, MV> mainEntity, Map<IEntityDef<?, ?>, FetcherEntity<?, ?>> entities) {
        this.mainEntity = mainEntity;
        this.entities = entities;
    }

    /**
     * Create a QueryDef builder.
     *
     * @param mainEntity main entity definition
     * @param <MK> main entity key class
     * @param <MV> main entity value class
     * @return builder
     */
    public static <MK, MV> Builder<MK, MV>.QueryEntityBuilder<MK, MV> builderFor(IEntityDef<MK, MV> mainEntity) {
        return new Builder<>(mainEntity).entity(mainEntity);
    }

    /**
     * QueryDef builder.
     *
     * @param <MK> main entity key type
     * @param <MV> main entity value type
     */
    public static class Builder<MK, MV> {

        private final Map<IEntityDef<?, ?>, FetcherEntity<?, ?>> entities = new LinkedHashMap<>();
        private final IEntityDef<MK, MV> mainEntity;

        private Builder(IEntityDef<MK, MV> mainEntity) {
            this.mainEntity = mainEntity;
        }

        /**
         * Registers a linked entity definition.
         *
         * @param entityDef entity definition
         * @param <K> entity key class
         * @param <V> entity value class
         * @return QueryEntityBuilder for this entity
         */
        public <K, V> QueryEntityBuilder<K, V> entity(IEntityDef<K, V> entityDef) {
            return new QueryEntityBuilder<>(entityDef);
        }

        /**
         * Linked entity definition builder.
         *
         * @param <K> entity key type
         * @param <V> entity value type
         */
        public class QueryEntityBuilder<K, V> {

            private final IEntityDef<K, V> entityDef;
            private final Map<String, ISingleGetter<K, V, ?, ?>> singles = new HashMap<>();
            private final Set<ILinkDef<K, V, ?, ?>> detaches = new HashSet<>();
            private final Map<String, IMultiGetter<K, V, ?, ?>> multis = new HashMap<>();
            private final Map<String, IContextSingleGetter<K, V, ?>> attrs = new HashMap<>();
            private final Map<String, Function<DataWrap<K, V>, ?>> postattrs = new HashMap<>();
            private Comparator<DataWrap<K, V>> sort = null;

            private QueryEntityBuilder(IEntityDef<K, V> entityDef) {
                this.entityDef = entityDef;
            }

            /**
             * Registers a IMultiGetter to be fetched with this entity as child data.
             *
             * The child data will be available from the resulting DataWrap via {@link DataWrap#multi(String)}
             *
             * @param name        name for child data.
             * @param multigetter IMultiGetter for child data
             * @param <CK>        child entity key type
             * @param <CV>        child entity value type
             * @return this entity builder
             */
            public <CK, CV> QueryEntityBuilder<K, V> include(String name, IMultiGetter<K, V, CK, CV> multigetter) {
                multis.put(name, multigetter);
                return this;
            }

            /**
             * Registers a one-to-many link to be fetched with this entity as child data.
             *
             * The child data will be available as a List from the resulting DataWrap via {@link DataWrap#multiLink(IMultiLinkDef)}
             *
             * @param linkDef child link definition
             * @param <CK>    child entity key type
             * @param <CV>    child entity value type
             * @return this entity builder
             */
            public <CK, CV> QueryEntityBuilder<K, V> include(IMultiLinkDef<K, V, CK, CV> linkDef) {
                return include(linkDef.getKind(), linkDef);
            }

            /**
             * Registers a ISingleGetter to be fetched with this entity as child data.
             *
             * The child data will be available from the resulting DataWrap via {@code DataWrap#singleLink(ISingleGetter)}
             *
             * @param name        name for child data.
             * @param             <CK> child entity key type
             * @param             <CV> child entity value type
             * @param getter  ISingleGetter for child dat
             * @return this entity builder
             */
            public <CK, CV> QueryEntityBuilder<K, V> include(String name, ISingleGetter<K, V, CK, CV> getter) {
                singles.put(name, getter);
                return this;
            }

            /**
             * Registers a one-to-one link to be fetched with this entity as child data.
             *
             * The child data will be available from the resulting DataWrap via {@code DataWrap#multiLink(ISingleLinkDef)}
             *
             * @param linkDef child link definition
             * @param         <L> child entity value type
             * @return this entity builder
             */
            public <L> QueryEntityBuilder<K, V> include(ISingleLinkDef<K, V, ?, L> linkDef) {
                return include(linkDef.getKind(), linkDef);
            }

            /**
             * Registers a link to be detached from when deleting the entity.
             *
             * @param parentLink link to this entity to be disconnected when removing this entity
             * @param            <CK> child entity key type
             * @param            <CV> child entity value type
             * @return this entity builder
             */
            public <CK, CV> QueryEntityBuilder<K, V> detach(ISingleLinkDef<K, V, CK, CV> parentLink) {
                detaches.add(parentLink);
                return this;
            }

            /**
             * Registers a link to be detached from when deleting the entity.
             *
             * @param parentLink link to this entity to be disconnected when removing this entity
             * @param            <CK> child entity key type
             * @param            <CV> child entity value type
             * @return this entity builder
             */
            public <CK, CV> QueryEntityBuilder<K, V> detach(IBiManyToManyLinkDef<K, V, CK, CV> parentLink) {
                detaches.add(parentLink);
                return this;
            }

            /**
             * Registers an attribute to be added to entity's DataWrap when fetching.
             *
             * @param name                attribute name
             * @param                     <CV> attribute type
             * @param contextSingleGetter IContextSingleGetter that returns attribute value from context
             * @return this entity builder
             */
            public <CV> QueryEntityBuilder<K, V> attribute(String name, IContextSingleGetter<K, V, CV> contextSingleGetter) {
                attrs.put(name, contextSingleGetter);
                return this;
            }

            /**
             * Registers an attribute to be added to entity's DataWrap when fetching.
             *
             * @param name     attribute name
             * @param function function that returns attribute value from DataWrap
             * @param          <CV> attribute type
             * @return this entity builder
             */
            public <CV> QueryEntityBuilder<K, V> attribute(String name, Function<DataWrap<K, V>, CV> function) {
                postattrs.put(name, function);
                return this;
            }

            /**
             * Registers sort comparator for this entity.
             * DataWraps will be sorted using this comparator.
             *
             * @param comparator comparator
             * @return this entity builder
             */
            public QueryEntityBuilder<K, V> sort(Comparator<V> comparator) {
                return sortWraps(Comparator.comparing(DataWrap::getNode, comparator));
            }

            /**
             * Registers DataWrap sort comparator for this entity.
             * DataWraps will be sorted using this comparator.
             *
             * @param comparator comparator
             * @return this entity builder
             */
            public QueryEntityBuilder<K, V> sortWraps(Comparator<DataWrap<K, V>> comparator) {
                sort = comparator;
                return this;
            }

            /**
             * Finish building entity and continue with query builder.
             *
             * @return query definition builder
             */
            public Builder<MK, MV> done() {
                Builder.this.addEntity(new FetcherEntity<>(entityDef, multis, singles, detaches, attrs, postattrs, sort));
                return Builder.this;
            }

        }

        private <K, V> void addEntity(FetcherEntity<K, V> entity) {
            entities.put(entity.entityDef, entity);
        }

        /**
         * Build query definition.
         * @return query definition
         */
        public JoinDef<MK, MV> build() {
            return new JoinDef<>(mainEntity, entities);
        }

    }

    private static class FetcherEntity<K, V> {

        private final IEntityDef<K, V> entityDef;
        private final Map<String, IMultiGetter<K, V, ?, ?>> multis;
        private final Map<String, ISingleGetter<K, V, ?, ?>> singles;
        private final Set<ILinkDef<K, V, ?, ?>> detaches;
        private final Comparator<DataWrap<K, V>> sort;
        private final Map<String, IContextSingleGetter<K, V, ?>> attrs;
        private final Map<String, Function<DataWrap<K, V>, ?>> postattrs;

        private FetcherEntity(IEntityDef<K, V> entityDef,
                              Map<String, IMultiGetter<K, V, ?, ?>> multis,
                              Map<String, ISingleGetter<K, V, ?, ?>> singles,
                              Set<ILinkDef<K, V, ?, ?>> detaches,
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

    /**
     * Get entity with all the linked data by its key.
     *
     * @param velvet velvet handle
     * @param key entity key
     * @return DataWrap for entity value with all the linked data, or null if no entity exists for the key.
     */
    public DataWrap<MK, MV> get(IVelvet velvet, MK key) {
        MV node = mainEntity.get(velvet, key);
        DataWrap<MK, MV> wrap = new BatchBuilder<>(velvet, mainEntity).make(node);
        return wrap;
    }

    /**
     * Gets multiple entities with all the linked data by their keys.
     *
     * When no entity exists for a key, the corresponding DataWrap will be silently missing from the resulting list.
     *
     * @param velvet velvet handle
     * @param keys entity keys
     * @return DataWrap for entity value with all the linked data
     */
    public List<DataWrap<MK, MV>> batchGet(IVelvet velvet, List<MK> keys) {
        List<MV> nodes = mainEntity.batchGetList(velvet, keys);
        return wrapNodes(velvet, mainEntity, nodes);
    }

    /**
     * Gets all the entities of this kind with all the linked data.
     *
     * @param velvet velvet handle
     * @return list of DataWraps
     */
    public List<DataWrap<MK, MV>> batchGetAll(IVelvet velvet) {
        List<MV> nodes = mainEntity.batchGetAllList(velvet);
        return wrapNodes(velvet, mainEntity, nodes);
    }

    /**
     * Deletes an entity by a key, removing or detaching the linked data.
     *
     * @param velvet velvet handle
     * @param key entity key
     */
    public void deleteKey(IVelvet velvet, MK key) {
        delete(velvet, mainEntity, key);
    }

    /**
     * Deletes an entity by a value, removing or detaching the linked data.
     *
     * @param velvet velvet handle
     * @param value entity value
     */
    public void deleteValue(IVelvet velvet, MV value) {
        delete(velvet, mainEntity, mainEntity.keyOf(value));
    }

    private <K, V> void delete(IVelvet velvet, IEntityDef<K, V> entityDef, K key) {
        @SuppressWarnings("unchecked")
        FetcherEntity<K, V> entity = (FetcherEntity<K, V>) entities.get(entityDef);
        if (entity != null) {
            for (Entry<String, IMultiGetter<K, V, ?, ?>> entry : entity.multis.entrySet()) {
                killChildren(velvet, entry.getValue(), key);
            }
            for (Entry<String, ISingleGetter<K, V, ?, ?>> entry : entity.singles.entrySet()) {
                killChild(velvet, entry.getValue(), key);
            }
            for (ILinkDef<K, V, ?, ?> parent : entity.detaches) {
                detachParent(velvet, parent, key);
            }
        }
        entityDef.deleteKey(velvet, key);
    }

    /**
     * Wrap an entity value into a DataWrap
     * @param velvet velvet handle
     * @param node entity value
     * @return DataWrap
     */
    public DataWrap<MK, MV> wrap(IVelvet velvet, MV node) {
        return wrap(velvet, mainEntity, node);
    }

    private <K, V> DataWrap<K, V> wrap(IVelvet velvet, IEntityDef<K, V> entityDef, V node) {
        DataWrap<K, V> wrap = new BatchBuilder<>(velvet, entityDef).make(node);
        return wrap;
    }

    /**
     * Wrap multiple entities into  DataWraps
     * @param velvet velvet handle
     * @param nodes entity values
     * @return DataWrap list
     */
    public List<DataWrap<MK, MV>> wrapNodes(IVelvet velvet, List<MV> nodes) {
        return wrapNodes(velvet, mainEntity, nodes);
    }

    private <K, V> List<DataWrap<K, V>> wrapNodes(IVelvet velvet, IEntityDef<K, V> entityDef, List<V> nodes) {
        List<DataWrap<K, V>> wrapList = new BatchBuilder<>(velvet, entityDef).make(nodes);
        return wrapList;
    }

    private <K, V, CK, CV> void killChild(IVelvet velvet, ISingleGetter<K, V, CK, CV> singleGetter, K key) {
        if (singleGetter instanceof ISingleLinkDef<?, ?, ?, ?>) {
            ISingleLinkDef<K, V, CK, CV> singleLinkDef = (ISingleLinkDef<K, V, CK, CV>) singleGetter;
            CK childKey = singleLinkDef.key(velvet, key);
            if (childKey != null) {
                singleLinkDef.disconnectKeys(velvet, key, childKey);
                delete(velvet, singleLinkDef.getChildEntity(), childKey);
            }
        }
    }

    private <K, V, CK, CV> void killChildren(IVelvet velvet, IMultiGetter<K, V, CK, CV> multiGetter, K key) {
        if (multiGetter instanceof IMultiLinkDef<?, ?, ?, ?>) {
            IMultiLinkDef<K, V, CK, CV> multiLinkDef = (IMultiLinkDef<K, V, CK, CV>) multiGetter;
            List<CK> childKeys = multiLinkDef.keys(velvet, key);
            for (CK childKey : childKeys) {
                multiLinkDef.disconnectKeys(velvet, key, childKey);
            }
            for (CK childKey : childKeys) {
                delete(velvet, multiLinkDef.getChildEntity(), childKey);
            }
        }
    }

    private <K, V, CK, CV> void detachParent(IVelvet velvet, ILinkDef<K, V, CK, CV> parentLinkDef, K key) {
        IMultiGetter<K, V, CK, CV> multiGetter = Links.toMultiGetter(parentLinkDef);
        List<CK> keys = multiGetter.keys(velvet, key);
        for (CK parentKey : keys) {
            parentLinkDef.disconnectKeys(velvet, key, parentKey);
        }
    }

    private <K, V> Stream<DataWrap<K, V>> sortWraps(IEntityDef<K, V> entityDef, Stream<DataWrap<K, V>> stream) {
        @SuppressWarnings("unchecked")
        FetcherEntity<K, V> fetcher = (FetcherEntity<K, V>) entities.get(entityDef);
        Comparator<DataWrap<K, V>> comparator = (fetcher == null) ? null : fetcher.sort;
        if (comparator != null) {
            stream = stream.sorted(comparator);
        }
        return stream;
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

        private void setKey(K key) {
            this.currentKey = key;
        }

        private void addWrap(DataWrap<K, V> wrap) {
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

        private Map<ISingleGetter<?, ?, ?, ?>, Map<?, ?>> singles = new HashMap<>();
        private Map<IMultiGetter<?, ?, ?, ?>, Map<?, List<?>>> multis = new HashMap<>();

        private BatchBuilder(IVelvet velvet, IEntityDef<KK, VV> startEntity) {
            this.velvet = velvet;
            this.startEntity = startEntity;
        }

        private DataWrap<KK, VV> make(VV node) {
            if (node == null) {
                return null;
            }
            preFetch(startEntity, Arrays.asList(node));
            DataWrap<KK, VV> wrap = wrap(startEntity, node, null);
            return wrap;
        }

        private List<DataWrap<KK, VV>> make(List<VV> startNodes) {
            preFetch(startEntity, startNodes);
            Stream<DataWrap<KK, VV>> wrapstream = startNodes.stream().map(node -> wrap(startEntity, node, null));
            wrapstream = sortWraps(startEntity, wrapstream);
            return wrapstream.collect(Collectors.toList());
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

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private <K, V, CK, CV> void preFetchSingle(ISingleGetter<K, V, CK, CV> single, List<V> nodes) {
            Map<K, CV> children = single.batchGet(velvet, nodes);
            singles.computeIfAbsent(single, s -> new LinkedHashMap<>()).putAll((Map) children);
            List<CV> childnodes = children.values().stream().filter(v -> v != null).collect(Collectors.toList());
            preFetch(single.getChildEntity(), childnodes);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private <K, V, CK, CV> void preFetchMulti(IMultiGetter<K, V, CK, CV> multi, List<V> nodes) {
            Map<K, List<CV>> children = multi.batchGet(velvet, nodes);
            multis.computeIfAbsent(multi, m -> new LinkedHashMap<>()).putAll((Map) children);
            List<CV> childnodes = children.values().stream().flatMap(List::stream).collect(Collectors.toList());
            preFetch(multi.getChildEntity(), childnodes);
        }

        private <K, V> DataWrap<K, V> wrap(IEntityDef<K, V> entityDef, V node, Context<?, ?> parentContext) {
            Context<K, V> context = new Context<>(parentContext, entityDef, node);
            
            K key = entityDef.keyOf(node);
            DataWrap.Builder<K, V> wrapBuilder = new DataWrap.Builder<K, V>(node).key(key);
            context.setKey(key);
            
            @SuppressWarnings("unchecked")
            FetcherEntity<K, V> entity = (FetcherEntity<K, V>) entities.get(entityDef);
            if (entity == null) {
                return wrapBuilder.build();
            }
            
            if (entity != null) {
                for (Entry<String, ? extends IMultiGetter<K, V, ?, ?>> entry : entity.multis.entrySet()) {
                    IMultiGetter<K, V, ?, ?> multiLinkDef = entry.getValue();
                    List<? extends DataWrap<?, ?>> wrappedLinks = wrapChildren(context, key, multiLinkDef);
                    wrapBuilder.addList(entry.getKey(), wrappedLinks);
                }
                for (Entry<String, ? extends ISingleGetter<K, V, ?, ?>> entry : entity.singles.entrySet()) {
                    ISingleGetter<K, V, ?, ?> singleConn = entry.getValue();
                    DataWrap<?, ?> wrappedLink = wrapChild(context, key, singleConn);
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
            Comparator<DataWrap<CK, CV>> comparator = (childFetcher == null) ? null : childFetcher.sort;
            @SuppressWarnings("unchecked")
            List<CV> cvs = (List<CV>) multis.get(multi).get(key);
            Stream<DataWrap<CK, CV>> stream = cvs.stream().map(o -> wrap(multi.getChildEntity(), o, context));
            if (comparator != null) {
                stream = stream.sorted(comparator);
            }
            return stream.collect(Collectors.toList());
        }

    }

}
