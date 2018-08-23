package com.zakgof.db.velvet.entity;

import java.util.List;
import java.util.Map;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.query.IKeyQuery;
import com.zakgof.db.velvet.query.ISingleReturnKeyQuery;

/**
 * Sortable entity definition - specifies a kind of entities with values represented by class V with a sortable primary key K
 *
 * @param <K> primary key type
 * @param <V> value type
 */
public interface ISortableEntityDef<K extends Comparable<? super K>, V> extends IEntityDef<K, V> {

    // TODO: USE SAME API FOR SECONDARY KEY QUERIES ?

    /**
     * Get a single entity key using primary key range query.
     *
     * If multiple entries match the query, an exception will be thrown. If no entity matches, null will be returned.
     *
     * @param velvet velvet handle
     * @param query primary key range query
     * @return entity key
     */
    public K queryKey(IVelvet velvet, ISingleReturnKeyQuery<K> query);

    /**
     * Get a single entity using primary key range query.
     *
     * If multiple entries match the query, an exception will be thrown. If no entity matches, null will be returned.
     *
     * @param velvet velvet handle
     * @param query primary key range query
     * @return entity
     */
    public default V queryValue(IVelvet velvet, ISingleReturnKeyQuery<K> query) {
        K key = queryKey(velvet, query);
        return key == null ? null : get(velvet, key);
    }

    // TODO: USE SAME API FOR SECONDARY KEY QUERIES ?

    /**
     * Get entity keys using primary key range query.
     *
     * @param velvet velvet handle
     * @param query primary key range query
     * @return list of entity keys
     */
    public List<K> queryKeys(IVelvet velvet, IKeyQuery<K> query);

    /**
     * Get entities using primary key range query.
     *
     * @param velvet velvet handle
     * @param query primary key range query
     * @return list of entities
     */
    public default List<V> queryList(IVelvet velvet, IKeyQuery<K> query) {
        return batchGetList(velvet, queryKeys(velvet, query));
    }

    /**
     * Get entities as key to value map using primary key range query.
     *
     * @param velvet velvet handle
     * @param query
     * @return key to value map of entities
     */
    public default Map<K, V> queryMap(IVelvet velvet, IKeyQuery<K> query) {
        return batchGetMap(velvet, queryKeys(velvet, query));
    }

}
