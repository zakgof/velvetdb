package com.zakgof.db.velvet.join;

import java.util.*;

import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IMultiLinkDef;
import com.zakgof.db.velvet.link.ISingleLinkDef;

public class DataWrap<K, V> {

    public static class Builder<K, V> {

        private K key;
        private final V node;
        private final Map<String, List<? extends DataWrap<?, ?>>> multis = new HashMap<>();
        private final Map<String, DataWrap<?, ?>> singles = new HashMap<>();
        private final Map<String, Object> attrs = new HashMap<>();

        public Builder(V node) {
            this.node = node;
        }

        public Builder(DataWrap<K, V> wrap) {
            this.node = wrap.node;
            this.key = wrap.key;
            this.multis.putAll(wrap.multis);
            this.singles.putAll(wrap.singles);
            this.attrs.putAll(wrap.attrs);
        }

        public Builder<K, V> addList(String name,List<? extends DataWrap<?, ?>> wrapperLinks) {
            multis.put(name, wrapperLinks);
            return this;
        }

        public Builder<K, V> add(String name, DataWrap<?, ?> childWrap) {
            singles.put(name, childWrap);
            return this;
        }

        public Builder<K, V> attr(String name, Object object) {
            attrs.put(name, object);
            return this;
        }

        public Builder<K, V> key(K key) {
            this.key = key;
            return this;
        }

        public DataWrap<K, V> build() {
            return new DataWrap<>(node, key, singles, multis, attrs);
        }
    }

    private final V node;
    private final K key;
    private final Map<String, DataWrap<?, ?>> singles;
    private final Map<String, List<? extends DataWrap<?, ?>>> multis;
    private final Map<String, Object> attrs;

    public V getNode() {
        return node;
    }

    public K getKey() {
        return key;
    }

    public DataWrap(V node, K key, Map<String, DataWrap<?, ?>> singles, Map<String, List<? extends DataWrap<?, ?>>> multis, Map<String, Object> attrs) {
        this.node = node;
        this.key = key;
        this.singles = singles;
        this.multis = multis;
        this.attrs = attrs;
    }

    public DataWrap(V node, IEntityDef<K, V> entity) {
        this.node = node;
        this.key = entity.keyOf(node);
        this.singles = Collections.emptyMap();
        this.multis = Collections.emptyMap();
        this.attrs = Collections.emptyMap();
    }

    public DataWrap(V node, K key) {
        this.node = node;
        this.key = key;
        this.singles = Collections.emptyMap();
        this.multis = Collections.emptyMap();
        this.attrs = Collections.emptyMap();
    }

    public List<? extends DataWrap<?, ?>> multi(String name) {
        return multis.get(name);
    }

    @SuppressWarnings("unchecked")
    public <CK, CV> List<DataWrap<CK, CV>> multiLink(IMultiLinkDef<K, V, CK, CV> link) {
        return (List<DataWrap<CK, CV>>) (List<?>) multis.get(link.getKind());
    }

    @SuppressWarnings("unchecked")
    public <CK, CV> DataWrap<CK, CV> single(String name) {
        return (DataWrap<CK, CV>) singles.get(name);
    }

    @SuppressWarnings("unchecked")
    public <CK, CV> DataWrap<CK, CV> singleLink(ISingleLinkDef<K, V, CK, CV> link) {
        return (DataWrap<CK, CV>) singles.get(link.getKind());
    }

    @SuppressWarnings("unchecked")
    public <CK, CV> CV singleNode(ISingleLinkDef<K, V, CK, CV> linkDef) {
        return (CV) singles.get(linkDef.getKind()).getNode();
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

    private String valueString(DataWrap<?, ?> wrap) {
        return wrap.getNode().toString();
    }

    public DataWrap<K, V> attachAttr(String name, V value) {
        return new Builder<>(this).attr(name, value).build();
    }

}
