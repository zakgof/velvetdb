package com.zakgof.db.velvet.entity.index;

import com.zakgof.db.velvet.entity.IMultiGetter;
import com.zakgof.db.velvet.query.IIndexQuery;

public interface IIndexedMultiGetter<HK, HV, CK, CV, C extends Comparable<C>> {
  IMultiGetter<HK, HV, CK, CV> indexed(IIndexQuery<C> indexQuery);
}
