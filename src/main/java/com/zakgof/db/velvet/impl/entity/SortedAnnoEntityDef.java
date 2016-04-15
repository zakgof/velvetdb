package com.zakgof.db.velvet.impl.entity;

import java.util.Collection;

import com.zakgof.db.velvet.entity.ISortableEntityDef;
import com.zakgof.db.velvet.properties.IProperty;
import com.zakgof.db.velvet.properties.IPropertyAccessor;

public class SortedAnnoEntityDef<K extends Comparable<K>, V> extends SortedEntityDef<K, V>
		implements ISortableEntityDef<K, V>, IPropertyAccessor<K, V> {

	private AnnoKeyProvider<K, V> annoKeyProvider;

	public SortedAnnoEntityDef(Class<V> valueClass, AnnoKeyProvider<K, V> annoKeyProvider) {
		super(annoKeyProvider.getKeyClass(), valueClass, AnnoEntityDef.kindOf(valueClass), annoKeyProvider);
		this.annoKeyProvider = annoKeyProvider;
	}

	@Override
	public Collection<String> getProperties() {
		return annoKeyProvider.getProperties();
	}

	@Override
	public IProperty<?, V> get(String property) {
		return annoKeyProvider.get(property);
	}

	@Override
	public IProperty<K, V> getKey() {
		return annoKeyProvider.getKey();
	}
}
