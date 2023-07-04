package com.zakgof.velvet.entity;

/**
 * Sortable set entity definition - specifies a kind of entities with a whole entity value constituting a sortable key.
 *
 * @param <V> entity value type
 */
public interface ISortedSetEntityDef<V extends Comparable<? super V>> extends ISortedEntityDef<V, V>, ISetEntityDef<V>  {
}
