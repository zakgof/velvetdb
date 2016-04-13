package com.zakgof.db.velvet.link;

import com.zakgof.db.velvet.query.IIndexQuery;
import com.zakgof.db.velvet.query.ISingleReturnIndexQuery;

public interface IIndexedMultiGetter<HK, HV, CK, CV, M extends Comparable<? super M>> {
  IMultiGetter<HK, HV, CK, CV> indexed(IIndexQuery<CK, M> indexQuery);
  
  ISingleGetter<HK, HV, CK, CV> indexedSingle(ISingleReturnIndexQuery<CK, M> indexQuery);
}
