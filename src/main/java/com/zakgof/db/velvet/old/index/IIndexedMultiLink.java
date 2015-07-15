package com.zakgof.db.velvet.old.index;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.query.IIndexQuery;
import com.zakgof.db.velvet.old.IMultiLinkDef;

public interface IIndexedMultiLink<A, B, C extends Comparable<C>> extends IMultiLinkDef<A, B> {
  
  public <K> List<B> links(IVelvet velvet, A node, IIndexQuery<K> indexQuery);
  
}
