package com.zakgof.db.velvet.impl.entity;

import java.util.List;
import java.util.WeakHashMap;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.entity.IKeylessEntityDef;
import com.zakgof.db.velvet.properties.IProperty;
import com.zakgof.db.velvet.properties.IPropertyAccessor;

public class KeylessEntityDef<V> extends SortedEntityDef<Long, V> implements IKeylessEntityDef<V> {

  private final WeakHashMap<V, Long> keys = new WeakHashMap<>();
  private IPropertyAccessor<Long, V> propertyAccessor;

  public KeylessEntityDef(Class<V> valueClass, String kind, List<IStoreIndexDef<?, V>> indexes) {
    super(Long.class, valueClass, kind, null, indexes);
    setKeyProvider(v -> keys.get(v));
    propertyAccessor = new KeylessPropertyProvider(valueClass);
  }

  @Override
  public Long keyOf(V value) {
    return keys.get(value);
  }

  @Override
  public V get(IVelvet velvet, Long key) {
    V value = super.get(velvet, key);
    keys.put(value, key);
    return value;
  }

  @Override
  public void put(IVelvet velvet, V value) {
    Long key = keys.get(value);
    if (key == null) {
      key = store(velvet).put(value);
    } else {
      store(velvet).put(key, value);
    }
    keys.put(value, key);
  }

  @Override
  public void deleteValue(IVelvet velvet, V value) {
    super.deleteValue(velvet, value);
    keys.remove(value);
  }

  private class KeylessPropertyProvider extends AnnoKeyProvider<Long, V> implements IPropertyAccessor<Long, V> {

    private IProperty<Long, V> keyProperty;

    public KeylessPropertyProvider(Class<V> valueClass) {
      super(valueClass);
      this.keyProperty = new IProperty<Long, V>() {

        @Override
        public Long get(V instance) {
          return keyOf(instance);
        }

        @Override
        public Class<Long> getType() {
          return Long.class;
        }

        @Override
        public String getName() {
          return "[autokey]";
        }
      };
    }

    @Override
    public IProperty<Long, V> getKey() {
      return keyProperty;
    }

  }

  @Override
  public IPropertyAccessor<Long, V> propertyAccessor() {
    return propertyAccessor;
  }

}
