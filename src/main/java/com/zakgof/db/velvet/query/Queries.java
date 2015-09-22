package com.zakgof.db.velvet.query;

public class Queries {
  
  public static IQuery create() {
    return new Query(-1, 0);
  }
  
  public static IQuery create(int limit, int offset) {
    return new Query(limit, offset);
  }
  
  public static <K extends Comparable<K>> Builder<K> builder() {
    return new Builder<>();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> range(K p1, boolean inclusive1, K p2, boolean inclusive2) {
    return Queries.<K>builder().from(new QueryAnchor<K>(inclusive1, p1)).to(new QueryAnchor<K>(inclusive2, p2)).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> greater(K p1) {
    return Queries.<K>builder().greater(p1).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> less(K p2) {
    return Queries.<K>builder().less(p2).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> greaterOrEq(K p1) {
    return Queries.<K>builder().greaterOrEq(p1).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> lessOrEq(K p2) {
    return Queries.<K>builder().lessOrEq(p2).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> equalsTo(K p) {
    return Queries.<K>builder().greaterOrEq(p).lessOrEq(p).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> first(int limit) {
    return Queries.<K>builder().limit(limit).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> first() {
    return first(1);
  }

  public static <K extends Comparable<K>> IIndexQuery<K> last(int limit) {
    return Queries.<K>builder().descending().limit(limit).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> last() {
    return last(1);
  }

  public static <K extends Comparable<K>> IIndexQuery<K> prev(K key) {
    return Queries.<K>builder().less(key).descending().limit(1).build();
  }

  public static <K extends Comparable<K>> IIndexQuery<K> next(K key) {
    return Queries.<K>builder().greater(key).limit(1).build();
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
      return new IndexQuery<>(lowAnchor, highAnchor, limit, offset, ascending);
    }
  }
  
  private static class Query implements IQuery {
    
    private Query(int limit, int offset) {
      this.limit = limit;
      this.offset = offset;
    }
    
    private final int offset;
    private final int limit;
    
    @Override
    public int getLimit() {
      return limit;
    }
    
    @Override
    public int getOffset() {
      return offset;
    }
  }

  private static class IndexQuery<K extends Comparable<K>> extends Query implements IIndexQuery<K> {

    private final boolean ascending;    
    private final IQueryAnchor<K> highAnchor;
    private final IQueryAnchor<K> lowAnchor;

    public IndexQuery(IQueryAnchor<K> lowAnchor, IQueryAnchor<K> highAnchor, int limit, int offset, boolean ascending) {
      super(limit, offset);
      this.lowAnchor = lowAnchor;
      this.highAnchor = highAnchor;
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
    public boolean isAscending() {
      return ascending;
    }

  }

}

class QueryAnchor<K extends Comparable<K>> implements IQueryAnchor<K> {

  private final boolean including;
  
  private final K value;

  public QueryAnchor(K value) {
    this(true, value);
  }

  public QueryAnchor(boolean including, K value) {
    this.including = including;
    this.value = value;
  }

  @Override
  public boolean isIncluding() {
    return including;
  }

  @Override
  public K getKey() {
    return value;
  }

}
