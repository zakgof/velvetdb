package com.zakgof.db.graph.datadef;

import com.zakgof.db.graph.IPersister;

public interface ISingleGetter<A, B> extends IGetter<A, B> {

  public B single(IPersister persister, A node);

  public Object singleKey(IPersister persister, Object key);
}
