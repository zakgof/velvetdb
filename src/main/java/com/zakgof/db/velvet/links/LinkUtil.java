package com.zakgof.db.velvet.links;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IndexQuery;
import com.zakgof.db.velvet.links.index.IIndexedMultiLink;

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
  
  public static <A, B> ISingleLinkDef<A, B> toSingleGetter(final IMultiLinkDef<A, B> multi) {
    return new ISingleLinkDef<A, B>() {
      @Override
      public B single(IVelvet velvet, A node) {
        List<B> links = multi.links(velvet, node);
        if (links.isEmpty())
          return null;
        if (links.size() == 1)
          return links.get(0);
        throw new RuntimeException("Multigetter return more than 1 entry and cannot be adapted to singlegetter " + links);
      }

      @Override
      public String getKind() {
        return multi.getKind();
      }

      @Override
      public Class<A> getHostClass() {
        return multi.getHostClass();
      }

      @Override
      public Class<B> getChildClass() {
        return multi.getChildClass();
      }

      @Override
      public void connect(IVelvet velvet, A a, B b) {
        multi.connect(velvet, a, b);
      }

      @Override
      public void connectKeys(IVelvet velvet, Object akey, Object bkey) {
        multi.connectKeys(velvet, akey, bkey);
      }

      @Override
      public void disconnect(IVelvet velvet, A a, B b) {
        multi.disconnect(velvet, a, b);
      }

      @Override
      public void disconnectKeys(IVelvet velvet, Object akey, Object bkey) {
        multi.disconnectKeys(velvet, akey, bkey);
      }

      
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
  
  public static <A, B, C extends Comparable<C>> IMultiLinkDef<A, B> toMultiGetter(final IIndexedMultiLink<A, B, C> indexedGetter, final IndexQuery<B, C> indexQuery) {
    
    return new IMultiLinkDef<A, B>() {

      @Override
      public List<B> links(IVelvet velvet, A node) {
        return indexedGetter.links(velvet, node, indexQuery);
      }

      @Override
      public String getKind() {
        return indexedGetter.getKind();
      }

      @Override
      public Class<A> getHostClass() {
        return indexedGetter.getHostClass();
      }

      @Override
      public Class<B> getChildClass() {
        return indexedGetter.getChildClass();
      }

      @Override
      public void connect(IVelvet velvet, A a, B b) {
        indexedGetter.connect(velvet, a, b);
      }

      @Override
      public void connectKeys(IVelvet velvet, Object akey, Object bkey) {
        indexedGetter.connectKeys(velvet, akey, bkey);
      }

      @Override
      public void disconnect(IVelvet velvet, A a, B b) {
        indexedGetter.disconnect(velvet, a, b);
      }

      @Override
      public void disconnectKeys(IVelvet velvet, Object akey, Object bkey) {
        indexedGetter.disconnectKeys(velvet, akey, bkey);
      }

    };
  }
  
  public static <A, B, C extends Comparable<C>> ISingleLinkDef<A, B> toSingleGetter(final IIndexedMultiLink<A, B, C> indexedGetter, final IndexQuery<B, C> indexQuery) {
    return toSingleGetter(toMultiGetter(indexedGetter, indexQuery));
  }
     

}
