package com.zakgof.db.velvet.link;

import com.zakgof.db.velvet.query.IKeyQuery;
import com.zakgof.db.velvet.query.ISingleReturnKeyQuery;

public interface IPriMultiGetter<HK, HV, CK extends Comparable<? super CK>, CV> {

    IMultiGetter<HK, HV, CK, CV> indexed(IKeyQuery<CK> indexQuery);

    ISingleGetter<HK, HV, CK, CV> indexedSingle(ISingleReturnKeyQuery<CK> indexQuery);

}
