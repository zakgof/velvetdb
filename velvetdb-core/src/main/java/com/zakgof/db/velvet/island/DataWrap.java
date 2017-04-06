package com.zakgof.db.velvet.island;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IMultiLinkDef;
import com.zakgof.db.velvet.link.ISingleLinkDef;

public class DataWrap<T> {

    public static class Builder<T> {

        private Object key;
        private final T node;
        private final Map<String, List<DataWrap<?>>> multis = new HashMap<>();
        private final Map<String, DataWrap<?>> singles = new HashMap<>();
        private final Map<String, Object> attrs = new HashMap<>();

        public Builder(T node) {
            this.node = node;
        }

        public Builder(DataWrap<T> wrap) {
            this.node = wrap.node;
            this.multis.putAll(wrap.multis);
            this.singles.putAll(wrap.singles);
        }

        public Builder<T> addList(String name, List<DataWrap<?>> wrapperLinks) {
            multis.put(name, wrapperLinks);
            return this;
        }

        public Builder<T> add(String name, DataWrap<?> childWrap) {
            singles.put(name, childWrap);
            return this;
        }

        public Builder<T> attr(String name, Object object) {
            attrs.put(name, object);
            return this;
        }

        public Builder<T> key(Object key) {
            this.key = key;
            return this;
        }

        public DataWrap<T> build() {
            return new DataWrap<>(node, key, singles, multis, attrs);
        }
    }

    private final T node;
    private final Object key;
    private final Map<String, DataWrap<?>> singles;
    private final Map<String, List<DataWrap<?>>> multis;
    private final Map<String, Object> attrs;

    public T getNode() {
        return node;
    }

    public Object getKey() {
        return key;
    }

    public DataWrap(T node, Object key, Map<String, DataWrap<?>> singles, Map<String, List<DataWrap<?>>> multis, Map<String, Object> attrs) {
        this.node = node;
        this.key = key;
        this.singles = singles;
        this.multis = multis;
        this.attrs = attrs;
    }

    public <K> DataWrap(T node, IEntityDef<K, T> entity) {
        this.node = node;
        this.key = entity.keyOf(node);
        this.singles = Collections.emptyMap();
        this.multis = Collections.emptyMap();
        this.attrs = Collections.emptyMap();
    }

    public DataWrap(T node, Object key) {
        this.node = node;
        this.key = key;
        this.singles = Collections.emptyMap();
        this.multis = Collections.emptyMap();
        this.attrs = Collections.emptyMap();
    }

    public List<DataWrap<?>> multi(String name) {
        return multis.get(name);
    }

    @SuppressWarnings("unchecked")
    public <L> List<DataWrap<L>> multiLink(IMultiLinkDef<?, T, ?, L> link) {
        return (List<DataWrap<L>>) (List<?>) multis.get(link.getKind());
    }

    public DataWrap<?> single(String name) {
        return singles.get(name);
    }

    @SuppressWarnings("unchecked")
    public <L> DataWrap<L> singleLink(ISingleLinkDef<?, T, ?, L> link) {
        return (DataWrap<L>) singles.get(link.getKind());
    }

    @SuppressWarnings("unchecked")
    public <L> L singleNode(ISingleLinkDef<?, T, ?, L> linkDef) {
        return (L) singles.get(linkDef.getKind()).getNode();
    }

    @SuppressWarnings("unchecked")
    public <L> L attr(String name) {
        return (L) attrs.get(name);
    }

    @Override
    public String toString() {
        return " " + node + " " + singles.entrySet().stream().reduce("", (s, e) -> s + e.getKey() + " [" + valueString(e.getValue()) + " ]", (s1, s2) -> s1 + s2)
               + multis.entrySet().stream().reduce("", (s, e) -> s + e.getKey() + " [" + e.getValue().size() + " ]", (s1, s2) -> s1 + s2);
    }

    private String valueString(DataWrap<?> wrap) {
        return wrap.getNode().toString();
    }

    public <V> DataWrap<T> attach(String name, V value) {
        return new Builder<>(this).add(name, new DataWrap<>(value, null)).build(); // TODO: manage key
    }

}
