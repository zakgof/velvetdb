package com.zakgof.db.velvet.api.query;

public class IndexQueryFactory {

  public static Builder builder() {
    return new Builder();
  }

  public static <M extends Comparable<M>> IIndexQuery rangeS(M p1, boolean inclusive1, M p2, boolean inclusive2) {
    return IndexQueryFactory.builder().from(new SecondaryIndexAnchor<M>(inclusive1, p1)).to(new SecondaryIndexAnchor<M>(inclusive2, p2)).build();
  }

  public static <M extends Comparable<M>> IIndexQuery greaterS(M p1) {
    return IndexQueryFactory.builder().greaterS(p1).build();
  }

  public static <M extends Comparable<M>> IIndexQuery lessS(M p2) {
    return IndexQueryFactory.builder().lessS(p2).build();
  }

  public static <M extends Comparable<M>> IIndexQuery greaterOrEqS(M p1) {
    return IndexQueryFactory.builder().greaterOrEqS(p1).build();
  }

  public static <M extends Comparable<M>> IIndexQuery lessOrEqS(M p2) {
    return IndexQueryFactory.builder().lessOrEqS(p2).build();
  }

  public static <B, M extends Comparable<M>> IIndexQuery equalsToS(M p) {
    return IndexQueryFactory.builder().greaterOrEqS(p).lessOrEqS(p).build();
  }

  /////

  public static <K extends Comparable<K>> IIndexQuery range(K p1, boolean inclusive1, K p2, boolean inclusive2) {
    return IndexQueryFactory.builder().from(new KeyAnchor<K>(inclusive1, p1)).to(new KeyAnchor<K>(inclusive2, p2)).build();
  }

  public static <K extends Comparable<K>> IIndexQuery greater(K p1) {
    return IndexQueryFactory.builder().greaterK(p1).build();
  }

  public static <K extends Comparable<K>> IIndexQuery less(K p2) {
    return IndexQueryFactory.builder().lessK(p2).build();
  }

  public static <K extends Comparable<K>> IIndexQuery greaterOrEq(K p1) {
    return IndexQueryFactory.builder().greaterOrEqK(p1).build();
  }

  public static <K extends Comparable<K>> IIndexQuery lessOrEq(K p2) {
    return IndexQueryFactory.builder().lessOrEqK(p2).build();
  }

  public static IIndexQuery first(int limit) {
    return IndexQueryFactory.builder().limit(limit).build();
  }

  public static IIndexQuery first() {
    return first(1);
  }

  public static IIndexQuery last() {
    return last(1);
  }

  public static IIndexQuery last(int limit) {
    return IndexQueryFactory.builder().descending().limit(limit).build();
  }

  public static <K extends Comparable<K>> IIndexQuery prev(K key) {
    return IndexQueryFactory.builder().lessK(key).descending().limit(1).build();
  }

  public static <K extends Comparable<K>> IIndexQuery next(K key) {
    return IndexQueryFactory.builder().greaterK(key).limit(1).build();
  }

  public static class Builder {

    private boolean ascending = true;
    private int offset = 0;
    private int limit = -1;
    private IQueryAnchor highAnchor;
    private IQueryAnchor lowAnchor;

    public <M extends Comparable<M>> Builder greaterS(M value) {
      this.lowAnchor = new SecondaryIndexAnchor<>(false, value);
      return this;
    }

    public <M extends Comparable<M>> Builder greaterOrEqS(M value) {
      this.lowAnchor = new SecondaryIndexAnchor<>(true, value);
      return this;
    }

    public <M extends Comparable<M>> Builder lessS(M value) {
      this.highAnchor = new SecondaryIndexAnchor<>(false, value);
      return this;
    }

    public <M extends Comparable<M>> Builder lessOrEqS(M value) {
      this.highAnchor = new SecondaryIndexAnchor<>(true, value);
      return this;
    }

    public <K extends Comparable<K>> Builder greaterK(K key) {
      this.lowAnchor = new KeyAnchor<>(false, key);
      return this;
    }

    public <K extends Comparable<K>> Builder greaterOrEqK(K key) {
      this.lowAnchor = new KeyAnchor<>(true, key);
      return this;
    }

    public <K extends Comparable<K>> Builder lessK(K key) {
      this.highAnchor = new KeyAnchor<>(false, key);
      return this;
    }

    public <K extends Comparable<K>> Builder lessOrEqK(K key) {
      this.highAnchor = new KeyAnchor<>(true, key);
      return this;
    }

    public Builder from(IQueryAnchor lowAnchor) {
      this.lowAnchor = lowAnchor;
      return this;
    }

    public Builder to(IQueryAnchor highAnchor) {
      this.highAnchor = highAnchor;
      return this;
    }

    public Builder offset(int offset) {
      this.offset = offset;
      return this;
    }

    public Builder limit(int limit) {
      this.limit = limit;
      return this;
    }

    public Builder descending() {
      this.ascending = false;
      return this;
    }

    public IIndexQuery build() {
      return new Query(lowAnchor, highAnchor, limit, offset, ascending);
    }
  }

  private static class Query implements IIndexQuery {

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

class KeyAnchor<K extends Comparable<K>> extends AbstractAnchor implements IKeyAnchor {

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
