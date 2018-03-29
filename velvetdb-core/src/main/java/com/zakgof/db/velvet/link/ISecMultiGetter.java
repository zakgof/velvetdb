package com.zakgof.db.velvet.link;

import com.zakgof.db.velvet.query.ISecQuery;
import com.zakgof.db.velvet.query.ISingleReturnSecQuery;

public interface ISecMultiGetter<HK, HV, CK, CV, M extends Comparable<? super M>> {

    IMultiGetter<HK, HV, CK, CV> indexed(ISecQuery<CK, M> indexQuery);

    ISingleGetter<HK, HV, CK, CV> indexedSingle(ISingleReturnSecQuery<CK, M> indexQuery);
   
}
