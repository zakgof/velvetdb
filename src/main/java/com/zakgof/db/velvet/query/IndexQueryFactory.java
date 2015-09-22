package com.zakgof.db.velvet.query;

public class IndexQueryFactory {
  
  public static <K extends Comparable<K>> Builder<K> builder() {
    return new Builder<>();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> range(K p1, boolean inclusive1, K p2, boolean inclusive2) {
    return IndexQueryFactory.<K>builder().from(new QueryAnchor<K>(inclusive1, p1)).to(new QueryAnchor<K>(inclusive2, p2)).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> greater(K p1) {
    return IndexQueryFactory.<K>builder().greater(p1).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> less(K p2) {
    return IndexQueryFactory.<K>builder().less(p2).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> greaterOrEq(K p1) {
    return IndexQueryFactory.<K>builder().greaterOrEq(p1).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> lessOrEq(K p2) {
    return IndexQueryFactory.<K>builder().lessOrEq(p2).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> equalsTo(K p) {
    return IndexQueryFactory.<K>builder().greaterOrEq(p).lessOrEq(p).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> first(int limit) {
    return IndexQueryFactory.<K>builder().limit(limit).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> first() {
    return first(1);
  }

  public static <K extends Comparable<K>> IIndexQuery<K> last(int limit) {
    return IndexQueryFactory.<K>builder().descending().limit(limit).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> last() {
    return last(1);
  }

  public static <K extends Comparable<K>> IIndexQuery<K> prev(K key) {
    return IndexQueryFactory.<K>builder().less(key).descending().limit(1).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> next(K key) {
    return IndexQueryFactory.<K>builder().greater(key).limit(1).build();
  }

  public static class Builder<K extends Comparable<K>> {

    private boolean ascending = true;
    private int offset = 0;
    private int limit = -1;
    private IQueryAnchor<K> highAnchor;
    private IQueryAnchor<K> lowAnchor;

    public Builder<K> greater(K value) {
      this.lowAnchor = new QueryAnchor<>(false, value);
      return this;
    }

    public Builder<K> greaterOrEq(K key) {
      this.lowAnchor = new QueryAnchor<>(true, key);
      return this;
    }

    public Builder<K> less(K key) {
      this.highAnchor = new QueryAnchor<>(false, key);
      return this;
    }

    public Builder<K> lessOrEq(K key) {
      this.highAnchor = new QueryAnchor<>(true, key);
      return this;
    }

    public Builder<K> from(IQueryAnchor<K> lowAnchor) {
      this.lowAnchor = lowAnchor;
      return this;
    }

    public Builder<K> to(IQueryAnchor<K> highAnchor) {
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
      return new Query<>(lowAnchor, highAnchor, limit, offset, ascending);
    }
  }

  private static class Query<K extends Comparable<K>> implements IIndexQuery<K> {

    private final boolean ascending;
    private final int offset;
    private final int limit;
    private final IQueryAnchor<K> highAnchor;
    private final IQueryAnchor<K> lowAnchor;

    public Query(IQueryAnchor<K> lowAnchor, IQueryAnchor<K> highAnchor, int limit, int offset, boolean ascending) {
      this.lowAnchor = lowAnchor;
      this.highAnchor = highAnchor;
      this.limit = limit;
      this.offset = offset;
      this.ascending = ascending;
    }

    @Override
    public IQueryAnchor<K> getLowAnchor() {
      return lowAnchor;
    }

    @Override
    public IQueryAnchor<K> getHighAnchor() {
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
