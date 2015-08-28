package com.zakgof.db.velvet.api.link;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;

public class Links {
  
  public static <HK, HV, CK, CV> ISingleLinkDef<HK, HV, CK, CV> single(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    return new SingleLinkDef<>(hostEntity, childEntity);
  }

  public static <HK, HV, CK, CV> ISingleLinkDef<HK, HV, CK, CV> single(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
    return new SingleLinkDef<>(hostEntity, childEntity, edgeKind);
  }

  public static <HK, HV, CK, CV> IMultiLinkDef<HK, HV, CK, CV> multi(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    return new MultiLinkDef<>(hostEntity, childEntity);
  }

  public static <HK, HV, CK, CV> IMultiLinkDef<HK, HV, CK, CV> multi(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
    return new MultiLinkDef<>(hostEntity, childEntity, edgeKind);
  }

  public static <HK, HV, CK, CV> IBiSingleLinkDef<HK, HV, CK, CV> biSingle(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    return BiSingleLinkDef.create(hostEntity, childEntity);
  }
  
  public static <HK, HV, CK, CV> IBiSingleLinkDef<HK, HV, CK, CV> biSingle(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind, String backEdgeKind) {
    return BiSingleLinkDef.create(hostEntity, childEntity, edgeKind, backEdgeKind);
  }
  
  public static <HK, HV, CK, CV> IBiMultiLinkDef<HK, HV, CK, CV> biMulti(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    return BiMultiLinkDef.create(hostEntity, childEntity);
  }
  
  public static <HK, HV, CK, CV> IBiMultiLinkDef<HK, HV, CK, CV> biMulti(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind, String backEdgeKind) {
    return BiMultiLinkDef.create(hostEntity, childEntity, edgeKind, backEdgeKind);
  }
  
  public static <HK, HV, CK, CV> IBiManyToManyLinkDef<HK, HV, CK, CV> biManyToMany(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    return BiManyToManyLinkDef.create(hostEntity, childEntity);
  }

  public static <HK, HV, CK, CV> IBiManyToManyLinkDef<HK, HV, CK, CV> biManyToMany(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind, String backEdgeKind) {
    return BiManyToManyLinkDef.create(hostEntity, childEntity, edgeKind, backEdgeKind);
  }

  public static <HK, HV, CK, CV> void createChild(ILinkDef<HK, HV, CK, CV> linkDef, IVelvet velvet, HV a, CV b) {
    linkDef.getChildEntity().put(velvet, b);
    linkDef.connect(velvet, a, b);
  }

  public static <HK, HV, CK, CV> IMultiGetter<HK, HV, CK, CV> toMultiGetter(ILinkDef<HK, HV, CK, CV> linkDef) {
    if (linkDef instanceof IMultiLinkDef)
      return (IMultiLinkDef<HK, HV, CK, CV>) linkDef;
    if (linkDef instanceof ISingleLinkDef) {
      
    @SuppressWarnings("unchecked")
    ISingleGetter<HK, HV, CK, CV> single = (ISingleGetter<HK, HV, CK, CV>) linkDef;
      return new IMultiGetter<HK, HV, CK, CV>() {

        @Override
        public List<CV> multi(IVelvet velvet, HV node) {
          return Arrays.asList(single.single(velvet, node));
        }

        @Override
        public List<CK> multiKeys(IVelvet velvet, HK key) {
          return Arrays.asList(single.singleKey(velvet, key));
        }
      };
    }
    return null;
  }
  
  public static <HK, HV, CK, CV> ISingleGetter<HK, HV, CK, CV> toSingleGetter(final IMultiLinkDef<HK, HV, CK, CV> multi) {
    return new ISingleGetter<HK, HV, CK, CV>() {
      @Override
      public CV single(IVelvet velvet, HV node) {
        List<CV> links = multi.multi(velvet, node);
        if (links.isEmpty())
          return null;
        if (links.size() == 1)
          return links.get(0);
        throw new RuntimeException("Multigetter returns more than 1 entry and cannot be adapted to singlegetter " + links);
      }

      @Override
      public CK singleKey(IVelvet velvet, HK key) {
        List<CK> linkKeys = multi.multiKeys(velvet, key);
        if (linkKeys.isEmpty())
          return null;
        if (linkKeys.size() == 1)
          return linkKeys.get(0);
        throw new RuntimeException("Multigetter returns more than 1 key and cannot be adapted to singlegetter " + linkKeys);
      }
    };
  }

  public static <HK, HV, CK extends Comparable<CK>, CV> PriIndexMultiLinkDef<HK, HV, CK, CV> pri(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    return new PriIndexMultiLinkDef<>(hostEntity, childEntity);
  }
  
  public static <HK, HV, CK, CV, M extends Comparable<M>> SecIndexMultiLinkDef<HK, HV, CK, CV, M> sec(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Class<M> mclazz, Function<CV, M> metric) {
    return new SecIndexMultiLinkDef<>(hostEntity, childEntity, mclazz, metric);
  }

}
