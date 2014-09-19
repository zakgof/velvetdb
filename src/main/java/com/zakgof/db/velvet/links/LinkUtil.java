package com.zakgof.db.velvet.links;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.zakgof.db.velvet.IVelvet;

public class LinkUtil {

  public static <A, B> void addChild(ILinkDef<A, B> linkDef, IVelvet velvet, A a, B b) {
    velvet.put(b);
    linkDef.connect(velvet, a, b);
  }

  public static <A, B> IMultiGetter<A, B> toMultiGetter(ILinkDef<A, B> linkDef) {
    if (linkDef instanceof IMultiLinkDef)
      return (IMultiLinkDef<A, B>) linkDef;
    if (linkDef instanceof ISingleLinkDef)
      return new SingleToMultiAdapter<A, B>((ISingleLinkDef<A, B>) linkDef);
    return null;
  }

  private static class SingleToMultiAdapter<A, B> implements IMultiGetter<A, B> {

    private final ISingleLinkDef<A, B> single;

    public SingleToMultiAdapter(ISingleLinkDef<A, B> single) {
      this.single = single;
    }

    @Override
    public List<B> links(IVelvet velvet, A node) {
      B child = single.single(velvet, node);
      return child == null ? Collections.emptyList() : Arrays.asList(child);
    }

    @Override
    public List<Object> linkKeys(IVelvet velvet, Object key) {
      Object childKey = single.singleKey(velvet, key);
      return childKey == null ? Collections.emptyList() : Arrays.asList(childKey);
    }

  }

}
