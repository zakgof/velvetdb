package com.zakgof.db.velvet.impl;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class Utils {
	public static <K, V> Map<K, V> makeLinkedHashMap(Iterable<K> keys, Function<K, V> mapper) {
		LinkedHashMap<K, V> lhm = new LinkedHashMap<>();
		for (K key : keys) {
			lhm.put(key, mapper.apply(key));
		}
		return lhm;
	}
}
