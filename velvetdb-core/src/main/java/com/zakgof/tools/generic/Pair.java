package com.zakgof.tools.generic;

import java.util.function.Function;

public class Pair<K, V> {
    private final K key;
    private final V value;
    public Pair(K k, V v) {
        key = k;
        value = v;
    }
    public Pair(Pair<? extends K, ? extends V> entry) {
        this(entry.first(), entry.second());
    }

    public K first() {
        return key;
    }

    public V second() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Pair)) {
            return false;
        } else {
            Pair<?, ?> oP = (Pair<?, ?>) o;
            return (key == null ?
                    oP.key == null :
                    key.equals(oP.key)) &&
                (value == null ?
                 oP.value == null :
                 value.equals(oP.value));
        }
    }

    @Override
    public int hashCode() {
        int result = key == null ? 0 : key.hashCode();

        final int h = value == null ? 0 : value.hashCode();
        result = 37 * result + h ^ (h >>> 16);

        return result;
    }

    @Override
    public String toString() {
        return "[" + first() + ", " + second() + "]";
    }

    public static <K, V> Pair<K, V> create(K k, V v) {
        return new Pair<K, V>(k, v);
    }
    
    public <K2> Pair<K2, V> fixFirst(Function<K, K2> firstFixer) {
      return create(firstFixer.apply(key), value);
    }
    
    public <V2> Pair<K, V2> fixSecond(Function<V, V2> secondFixer) {
      return create(key, secondFixer.apply(value));
    }
}
