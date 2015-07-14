package com.zakgof.db.velvet.api.link;

import com.zakgof.db.velvet.api.query.IIndexQuery;

public interface IIndexedMultiGetter<HK, HV, CK, CV, C extends Comparable<C>> {
  IMultiGetter<HK, HV, CK, CV> indexed(IIndexQuery<CK> indexQuery);
}
