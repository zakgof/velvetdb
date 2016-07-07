package com.zakgof.db.velvet.impl.entity;

import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.ISortedStore;
import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.entity.ISortableEntityDef;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.ISingleReturnRangeQuery;

public class SortedEntityDef<K extends Comparable<? super K>, V> extends EntityDef<K, V> implements ISortableEntityDef<K, V> {
  
  public SortedEntityDef(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider, List<IStoreIndexDef<?, V>> indexes) {
    super(keyClass, valueClass, kind, keyProvider, indexes);
  }

	@Override
	public ISortedStore<K, V> store(IVelvet velvet) {
		return velvet.sortedStore(getKind(), getKeyClass(), getValueClass(), indexes);
	}

  @Override
  public List<K> keys(IVelvet velvet, IRangeQuery<K, K> query) {
    return store(velvet).keys(query);
  }
  
  @Override
  public K key(IVelvet velvet, ISingleReturnRangeQuery<K, K> query) {
    List<K> keys = keys(velvet, query);
    if (keys.size() > 1)
      throw new VelvetException("ISingleReturnIndexQuery returned multiple entries");
    return keys.isEmpty() ? null : keys.get(0);
  }
}
