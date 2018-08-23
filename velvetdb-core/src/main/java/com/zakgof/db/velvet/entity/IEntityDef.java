package com.zakgof.db.velvet.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.properties.IPropertyAccessor;
import com.zakgof.db.velvet.query.ISecQuery;
import com.zakgof.db.velvet.query.ISingleReturnSecQuery;

/**
 * Entity definition - specifies a kind of entities with values represented by class V
 *
 * @param <K> primary key type
 * @param <V> value type
 */
public interface IEntityDef<K, V> {

    // Metadata

    /**
     * @return key class
     */
    public Class<K> getKeyClass();

    /**
     * @return value class
     */
    public Class<V> getValueClass();

    /**
     * @return entity name
     */
    public String getKind();

    /**
     * @return property accessor
     */
    public IPropertyAccessor<K, V> propertyAccessor();

    // Read

    /**
     * Returns key for an entity value.
     * @param value entity
     * @return key
     */
    public K keyOf(V value);

    /**
     * Gets a single entity by a key.
     *
     * @param velvet velvet handle
     * @param key key
     * @return value or null if key not found
     */
    public V get(IVelvet velvet, K key);

    /**
     * Gets multiple entities for multiple keys as key to value map.
     *
     * @param velvet velvet handle
     * @param keys keys to query
     * @return key to value map; if no value found for a key, the corresponding entry will be missing from the map
     */
    public Map<K, V> batchGetMap(IVelvet velvet, List<K> keys);

    /**
     * Gets multiple entities for multiple keys.
     *
     * @param velvet velvet handle
     * @param keys keys to query
     * @return values in the order corresponding to the requested keys; if no value found for a key, a null value will be present at the corresponding position
     */
    default public List<V> batchGetList(IVelvet velvet, List<K> keys) {
        return new ArrayList<>(batchGetMap(velvet, keys).values());
    }

    /**
     * Gets all the entities of this kind as key to value map.
     *
     * @param velvet velvet handle
     * @return key to value map
     */
    public Map<K, V> batchGetAllMap(IVelvet velvet);


    /**
     * Gets all the entities of this kind.
     *
     * @param velvet velvet handle
     * @return value list
     */
    default public List<V> batchGetAllList(IVelvet velvet) {
        return new ArrayList<>(batchGetAllMap(velvet).values());
    }

    /**
     * Gets all entity keys.
     * @param velvet velvet handle
     * @return key list
     */
    public List<K> batchGetAllKeys(IVelvet velvet);

    /**
     * Gets the number of entities of this kind.
     * This method may be slow on some implementations.
     * @param velvet velvet handle
     * @return number of entities
     */
    public long size(IVelvet velvet);

    /**
     * Checks if an entity with the specified key exists.
     * This method may be slow on some implementations.
     * @param velvet velvet handle
     * @param key key to check
     * @return true if entity exists
     */
    public boolean containsKey(IVelvet velvet, K key);


    // Write

    /**
     * Adds or updates an entity.
     *
     * If an entity with the specified key exists, it will be updated, otherwise created.
     *
     * @param velvet velvet handle
     * @param value entity value
     * @return key of the added/updated entity.
     */
    public K put(IVelvet velvet, V value);

    /**
     * Adds or updates an entity with an explicit key.
     *
     * If an entity with the specified key exists, it will be updated, otherwise created.
     *
     * @param velvet velvet handle
     * @param key entity key
     * @param value entity value
     * @return key of the added/updated entity.
     */
    public K put(IVelvet velvet, K key, V value);

    /**
     * Adds or updates multiple entities.
     *
     * If an entity with the specified key exists, it will be updated, otherwise created.
     *
     * @param velvet velvet handle
     * @param values list of entities
     * @return keys of the added/updated entities.
     */
    public List<K> batchPut(IVelvet velvet, List<V> values);

    /**
     * Adds or updates multiple entities.
     *
     * If an entity with the specified key exists, it will be updated, otherwise created.
     *
     * @param velvet velvet handle
     * @param keys list of entity keys
     * @param values list of entities, must have the same length as list of entity keys
     * @return keys of the added/updated entities.
     */
    public List<K> batchPut(IVelvet velvet, List<K> keys, List<V> values);

    // Delete

    /**
     * Delete an entity by a key.
     * @param velvet velvet handle
     * @param key entity key
     */
    public void deleteKey(IVelvet velvet, K key);

    /**
     * Delete an entity by a value.
     *
     * If no entity exists with the given key, it will be ignored.
     *
     * @param velvet velvet handle
     * @param value entity value
     */
    public default void deleteValue(IVelvet velvet, V value) {
        deleteKey(velvet, keyOf(value));
    }

    /**
     * Delete multiple entities by keys.
     *
     * If no entity exist for any of the given keys, it will be ignored.
     *
     * @param velvet velvet handle
     * @param keys list of keys
     */
    public void batchDeleteKeys(IVelvet velvet, List<K> keys);

    /**
     * Delete multiple entities by values.
     *
     * If no entity exist for any of the given keys, it will be ignored.
     *
     * @param velvet velvet handle
     * @param values entities to delete
     */
    public default void batchDeleteValues(IVelvet velvet, List<V> values) {
        batchDeleteKeys(velvet, values.stream().map(this::keyOf).collect(Collectors.toList()));
    }

    // Secondary key range queries

    /**
     * Get a single entity key using secondary key query.
     *
     * If multiple entries match the query, an exception will be thrown. If no entity matches, null will be returned.
     *
     * @param velvet velvet handle
     * @param indexName secondary index name
     * @param query query
     * @param <M> index value type
     * @return matching entry key or null
     */
    public <M extends Comparable<? super M>> K queryKey(IVelvet velvet, String indexName, ISingleReturnSecQuery<K, M> query);

    /**
     * Get a single entity using secondary key query.
     *
     * If multiple entries match the query, an exception will be thrown. If no entity matches, null will be returned.
     *
     * @param velvet velvet handle
     * @param indexName secondary index name
     * @param query query
     * @param <M> index value type
     * @return matching entry or null
     */
    public <M extends Comparable<? super M>> V queryValue(IVelvet velvet, String indexName, ISingleReturnSecQuery<K, M> query);

    /**
     * Get entity keys using secondary key query.
     *
     * @param velvet velvet handle
     * @param indexName secondary index name
     * @param query query
     * @param <M> index value type
     * @return list of keys
     */
    public <M extends Comparable<? super M>> List<K> queryKeys(IVelvet velvet, String indexName, ISecQuery<K, M> query);

    // TODO multi-key queries ? (including primary + secondary keys mix)
    /**
     * Get entities using secondary key query.
     *
     * @param velvet velvet handle
     * @param indexName secondary index name
     * @param query query
     * @param <M> index value type
     * @return list of entities
     */
    public <M extends Comparable<? super M>> List<V> queryList(IVelvet velvet, String indexName, ISecQuery<K, M> query);

    /**
     * Get entities as key to value map using secondary key query.
     *
     * @param velvet velvet handle
     * @param indexName secondary index name
     * @param query query
     * @param <M> index value type
     * @return key to value entity map
     */
    public <M extends Comparable<? super M>> Map<K, V> queryMap(IVelvet velvet, String indexName, ISecQuery<K, M> query);

    // Other

    /**
     * Test if values are equal by comparing their keys.
     * @param value1
     * @param value2
     * @return true if keys are the same.
     */
    public default boolean equals(V value1, V value2) {
        return keyOf(value1).equals(keyOf(value2));
    }

}
