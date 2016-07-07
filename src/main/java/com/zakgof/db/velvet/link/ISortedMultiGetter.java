package com.zakgof.db.velvet.link;

import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.ISingleReturnRangeQuery;

public interface ISortedMultiGetter<HK, HV, CK, CV, M extends Comparable<? super M>> {
  IMultiGetter<HK, HV, CK, CV> indexed(IRangeQuery<CK, M> indexQuery);
  
  ISingleGetter<HK, HV, CK, CV> indexedSingle(ISingleReturnRangeQuery<CK, M> indexQuery);
}
