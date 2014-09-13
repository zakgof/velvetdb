package com.zakgof.db.graph.datadef;

import java.util.List;

import com.zakgof.db.graph.IPersister;

public interface IMultiGetter<A, B> extends IGetter<A, B> {

  public List<B> links(IPersister persister, A node);

  public List<Object> linkKeys(IPersister persister, Object key);

}
