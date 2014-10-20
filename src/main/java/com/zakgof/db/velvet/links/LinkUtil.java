package com.zakgof.db.velvet.links;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.links.index.IIndexedGetter;
import com.zakgof.db.velvet.links.index.IndexQuery;

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
  
  public static <A, B> ISingleGetter<A, B> toSingleGetter(final IMultiGetter<A, B> multi) {
    return new ISingleGetter<A, B>() {
      @Override
      public B single(IVelvet velvet, A node) {
        List<B> links = multi.links(velvet, node);
        if (links.isEmpty())
          return null;
        if (links.size() == 1)
          return links.get(0);
        throw new RuntimeException("Multigetter return more than 1 entry and cannot be adapted to singlegetter " + links);
      }

      /*
      @Override
      public Object singleKey(IVelvet velvet, Object key) {
        List<?> linkKeys = multi.linkKeys(velvet, key);
        if (linkKeys.isEmpty())
          return null;
        if (linkKeys.size() == 1)
          return linkKeys.get(0);
        throw new RuntimeException("Multigetter return more than 1 entry and cannot be adapted to singlegetter " + linkKeys);
      }
      */
    };
  }

  private static class SingleToMultiAdapter<A, B> implements IMultiGetter<A, B> {

    private final ISingleGetter<A, B> single;

    public SingleToMultiAdapter(ISingleGetter<A, B> single) {
      this.single = single;
    }

    @Override
    public List<B> links(IVelvet velvet, A node) {
      B child = single.single(velvet, node);
      return child == null ? Collections.emptyList() : Arrays.asList(child);
    }

    /*
    @Override
    public List<Object> linkKeys(IVelvet velvet, Object key) {
      Object childKey = single.singleKey(velvet, key);
      return childKey == null ? Collections.emptyList() : Arrays.asList(childKey);
    }
    */

  }
  
  public static <A, B, C extends Comparable<C>> IMultiGetter<A, B> toMultiGetter(final IIndexedGetter<A, B, C> indexedGetter, final IndexQuery<B, C> indexQuery) {
    return new IMultiGetter<A, B>() {

      @Override
      public List<B> links(IVelvet velvet, A node) {
        return indexedGetter.links(velvet, node, indexQuery);
      }

      /*
      @Override
      public List<Object> linkKeys(IVelvet velvet, Object key) {
        return indexedGetter.linkKeys(velvet, key, indexQuery);
      }
      */
    };
  }
  
  public static <A, B, C extends Comparable<C>> ISingleGetter<A, B> toSingleGetter(final IIndexedGetter<A, B, C> indexedGetter, final IndexQuery<B, C> indexQuery) {
    return toSingleGetter(toMultiGetter(indexedGetter, indexQuery));
  }
     

}
