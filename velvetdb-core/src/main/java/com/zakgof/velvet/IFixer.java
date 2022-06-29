package com.zakgof.db.velvet;

import java.util.Map;

public interface IFixer<T> {
    public T fix(T original, Map<String, Object> loadedFields);
}
