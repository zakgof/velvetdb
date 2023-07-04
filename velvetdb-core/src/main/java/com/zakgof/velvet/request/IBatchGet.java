package com.zakgof.velvet.request;

import java.util.List;
import java.util.Map;

public interface IBatchGet<K, V> {

    IReadCommand<List<K>> asKeyList();

    IReadCommand<List<V>> asValueList();

    IReadCommand<Map<K, V>> asMap();
}
