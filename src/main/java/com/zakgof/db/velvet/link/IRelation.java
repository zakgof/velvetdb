package com.zakgof.db.velvet.link;

import com.zakgof.db.velvet.entity.IEntityDef;

public interface IRelation<HK, HV, CK, CV> {

	public IEntityDef<HK, HV> getHostEntity();

	public IEntityDef<CK, CV> getChildEntity();
}
