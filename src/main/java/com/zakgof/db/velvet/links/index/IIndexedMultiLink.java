package com.zakgof.db.velvet.links.index;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IndexQuery;
import com.zakgof.db.velvet.links.IMultiLinkDef;

public interface IIndexedMultiLink<A, B, C extends Comparable<C>> extends IMultiLinkDef<A, B> {
  
  public <K> List<B> links(IVelvet velvet, A node, IndexQuery<K, C> indexQuery);
  
}
