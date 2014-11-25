package com.zakgof.db.velvet.links.index;

import static com.zakgof.db.velvet.VelvetUtil.kindOf;

import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IndexQuery;
import com.zakgof.db.velvet.links.BiMultiLinkDef;
import com.zakgof.db.velvet.links.IMultiLinkDef;

public class IndexedBiMultiLinkDef<A, B, C extends Comparable<C>> extends BiMultiLinkDef<A, B> implements IIndexedMultiLink<A, B, C> {

  public IndexedBiMultiLinkDef(Class<A> aClazz, Class<B> bClazz, Function<B, C> metrics) {
    super(IndexedMultiLinkDef.of(aClazz, bClazz, metrics), kindOf(bClazz) + "-" + kindOf(aClazz));
  }

  public static <A, B, C extends Comparable<C>> IndexedBiMultiLinkDef<A, B, C> of(Class<A> aClazz, Class<B> bClazz, Function<B, C> metrics) {
    return new IndexedBiMultiLinkDef<A, B, C>(aClazz, bClazz, metrics);
  }

  @Override
  public List<B> links(IVelvet velvet, A node, IndexQuery<B, C> indexQuery) {
    // TODO avoid cast by subclassing one more level: ABiMultiLinkDef
    return ((IndexedMultiLinkDef<A, B, C>)oneWay).links(velvet, node, indexQuery);
  }
  
  public IMultiLinkDef<A, B> indexGetter(final IndexQuery<B, C> indexQuery) {
    return ((IndexedMultiLinkDef<A, B, C>)oneWay).indexGetter(indexQuery);
  }

}
