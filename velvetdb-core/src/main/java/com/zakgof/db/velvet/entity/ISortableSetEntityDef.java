package com.zakgof.db.velvet.entity;

/**
 * Sortable set entity definition - specifies a kind of entities with a whole entity value constituting a sortable key.
 *
 * @param <V> entity value type
 */
public interface ISortableSetEntityDef<V extends Comparable<? super V>> extends ISortableEntityDef<V, V>, ISetEntityDef<V>  {
}
