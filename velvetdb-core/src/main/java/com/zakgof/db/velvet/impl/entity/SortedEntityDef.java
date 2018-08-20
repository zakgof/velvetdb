package com.zakgof.db.velvet.impl.entity;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.ISortedStore;
import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.entity.ISortableEntityDef;
import com.zakgof.db.velvet.query.IKeyQuery;
import com.zakgof.db.velvet.query.ISingleReturnKeyQuery;

public class SortedEntityDef<K extends Comparable<? super K>, V> extends EntityDef<K, V> implements ISortableEntityDef<K, V> {

    public SortedEntityDef(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider, Collection<IStoreIndexDef<?, V>> indexes) {
        super(keyClass, valueClass, kind, keyProvider, indexes);
    }

    @Override
    public ISortedStore<K, V> store(IVelvet velvet) {
        return velvet.sortedStore(getKind(), getKeyClass(), getValueClass(), indexes);
    }

    @Override
    public List<K> queryKeys(IVelvet velvet, IKeyQuery<K> query) {
        return store(velvet).keys(query);
    }

    @Override
    public K queryKey(IVelvet velvet, ISingleReturnKeyQuery<K> query) {
        List<K> keys = queryKeys(velvet, (IKeyQuery<K>)query);
        if (keys.size() > 1)
            throw new VelvetException("ISingleReturnIndexQuery returned multiple entries");
        return keys.isEmpty() ? null : keys.get(0);
    }
}
