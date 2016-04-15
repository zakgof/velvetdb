package com.zakgof.db.velvet.impl.entity;

import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.ISortedStore;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.entity.ISortableEntityDef;
import com.zakgof.db.velvet.query.IIndexQuery;
import com.zakgof.db.velvet.query.ISingleReturnIndexQuery;

public class SortedEntityDef<K extends Comparable<? super K>, V> extends EntityDef<K, V> implements ISortableEntityDef<K, V> {
  
  public SortedEntityDef(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
    super(keyClass, valueClass, kind, keyProvider);
  }

	@Override
	public ISortedStore<K, V> store(IVelvet velvet) {
		return velvet.sortedStore(getKind(), getKeyClass(), getValueClass());
	}

  @Override
  public List<K> keys(IVelvet velvet, IIndexQuery<K, K> query) {
    return store(velvet).keys(query);
  }
  
  @Override
  public K key(IVelvet velvet, ISingleReturnIndexQuery<K, K> query) {
    List<K> keys = keys(velvet, query);
    if (keys.size() > 1)
      throw new VelvetException("ISingleReturnIndexQuery returned multiple entries");
    return keys.isEmpty() ? null : keys.get(0);
  }
}
