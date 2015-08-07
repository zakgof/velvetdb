package com.zakgof.db.velvet.api.query;

public class IndexQueryFactory {

  public static <K> Builder<K> builder() {
    return new Builder<K>();
  }

  public static <B, M extends Comparable<M>> IIndexQuery<B> range(M p1, boolean inclusive1, M p2, boolean inclusive2) {
    return IndexQueryFactory.<B> builder().from(new SecondaryIndexAnchor<M>(inclusive1, p1)).to(new SecondaryIndexAnchor<M>(inclusive2, p2)).build();
  }

  public static <B, M extends Comparable<M>> IIndexQuery<B> greater(M p1) {
    return IndexQueryFactory.<B> builder().greaterS(p1).build();
  }

  public static <B, M extends Comparable<M>> IIndexQuery<B> less(M p2) {
    return IndexQueryFactory.<B> builder().lessS(p2).build();
  }

  public static <B, M extends Comparable<M>> IIndexQuery<B> greaterOrEq(M p1) {
    return IndexQueryFactory.<B> builder().greaterOrEqS(p1).build();
  }

  public static <B, M extends Comparable<M>> IIndexQuery<B> lessOrEq(M p2) {
    return IndexQueryFactory.<B> builder().lessOrEqS(p2).build();
  }

  public static <B> IIndexQuery<B> first(int limit) {
    return IndexQueryFactory.<B> builder().limit(limit).build();
  }

  public static <B> IIndexQuery<B> first() {
    return first(1);
  }

  public static <B> IIndexQuery<B> last() {
    return last(1);
  }

  public static <B> IIndexQuery<B> last(int limit) {
    return IndexQueryFactory.<B> builder().descending().limit(limit).build();
  }

  public static <B, M extends Comparable<M>> IIndexQuery<B> equalsTo(M p) {
    return IndexQueryFactory.<B> builder().greaterOrEqS(p).lessOrEqS(p).build();
  }

//  public static <T, K> IIndexQuery<K> prev(T node) {
//    @SuppressWarnings("unchecked")
//    K key = (K) VelvetUtil.keyOfValue(node);
//    return prevKey(key);
//  }

  public static <T, K> IIndexQuery<K> prevKey(K key) {
    return IndexQueryFactory.<K> builder().lessK(key).descending().limit(1).build();
  }

//  public static <T, K> IIndexQuery<K> next(T node) {
//    @SuppressWarnings("unchecked")
//    K key = (K) VelvetUtil.keyOfValue(node);
//    return nextKey(key);
//  }

  public static <T, K> IIndexQuery<K> nextKey(K key) {
    return IndexQueryFactory.<K> builder().greaterK(key).limit(1).build();
  }

  public static class Builder<K> {

    private boolean ascending = true;
    private int offset = 0;
    private int limit = -1;
    private IQueryAnchor highAnchor;
    private IQueryAnchor lowAnchor;

    public <M extends Comparable<M>> Builder<K> greaterS(M value) {
      this.lowAnchor = new SecondaryIndexAnchor<>(false, value);
      return this;
    }

    public <M extends Comparable<M>> Builder<K> greaterOrEqS(M value) {
      this.lowAnchor = new SecondaryIndexAnchor<>(true, value);
      return this;
    }

    public <M extends Comparable<M>> Builder<K> lessS(M value) {
      this.highAnchor = new SecondaryIndexAnchor<>(false, value);
      return this;
    }

    public <M extends Comparable<M>> Builder<K> lessOrEqS(M value) {
      this.highAnchor = new SecondaryIndexAnchor<>(true, value);
      return this;
    }

    public Builder<K> greaterK(K key) {
      this.lowAnchor = new KeyAnchor<>(false, key);
      return this;
    }

    public Builder<K> greaterOrEqK(K key) {
      this.lowAnchor = new KeyAnchor<>(true, key);
      return this;
    }

    public Builder<K> lessK(K key) {
      this.highAnchor = new KeyAnchor<>(false, key);
      return this;
    }

    public Builder<K> lessOrEqK(K key) {
      this.highAnchor = new KeyAnchor<>(true, key);
      return this;
    }

    public Builder<K> from(IQueryAnchor lowAnchor) {
      this.lowAnchor = lowAnchor;
      return this;
    }

    public Builder<K> to(IQueryAnchor highAnchor) {
      this.highAnchor = highAnchor;
      return this;
    }

    public Builder<K> offset(int offset) {
      this.offset = offset;
      return this;
    }

    public Builder<K> limit(int limit) {
      this.limit = limit;
      return this;
    }

    public Builder<K> descending() {
      this.ascending = false;
      return this;
    }

    public IIndexQuery<K> build() {
      return new Query<K>(lowAnchor, highAnchor, limit, offset, ascending);
    }
  }

  private static class Query<K> implements IIndexQuery<K> {

    private final boolean ascending;
    private final int offset;
    private final int limit;
    private final IQueryAnchor highAnchor;
    private final IQueryAnchor lowAnchor;

    public Query(IQueryAnchor lowAnchor, IQueryAnchor highAnchor, int limit, int offset, boolean ascending) {
      this.lowAnchor = lowAnchor;
      this.highAnchor = highAnchor;
      this.limit = limit;
      this.offset = offset;
      this.ascending = ascending;
    }

    @Override
    public IQueryAnchor getLowAnchor() {
      return lowAnchor;
    }

    @Override
    public IQueryAnchor getHighAnchor() {
      return highAnchor;
    }

    @Override
    public int getLimit() {
      return limit;
    }

    @Override
    public int getOffset() {
      return offset;
    }

    @Override
    public boolean isAscending() {
      return ascending;
    }

  }

}

class AbstractAnchor implements IQueryAnchor {

  private boolean including = true;

  public AbstractAnchor(boolean including) {
    this.including = including;
  }

  @Override
  public boolean isIncluding() {
    return including;
  }

}

class PositionAnchor extends AbstractAnchor implements IPositionAnchor {

  public PositionAnchor(boolean including, int position) {
    super(including);
    this.position = position;
  }

  private final int position;

  @Override
  public int getPosition() {
    return position;
  }

}

class KeyAnchor<K> extends AbstractAnchor implements IKeyAnchor<K> {

  public KeyAnchor(K key) {
    super(true);
    this.key = key;
  }

  public KeyAnchor(boolean including, K key) {
    super(including);
    this.key = key;
  }

  private final K key;

  @Override
  public K getKey() {
    return key;
  }

}

class SecondaryIndexAnchor<M extends Comparable<M>> extends AbstractAnchor implements ISecondaryIndexAnchor<M> {

  public SecondaryIndexAnchor(M value) {
    super(true);
    this.value = value;
  }

  public SecondaryIndexAnchor(boolean including, M value) {
    super(including);
    this.value = value;
  }

  private final M value;

  @Override
  public M getValue() {
    return value;
  }

}
