package com.zakgof.velvet.request;

import java.util.List;
import java.util.Map;

public interface IBatchGet<K, V> {
    IReadRequest<List<K>> asKeyList();
    IReadRequest<List<V>> asValueList();
    IReadRequest<Map<K, V>> asMap();
}
