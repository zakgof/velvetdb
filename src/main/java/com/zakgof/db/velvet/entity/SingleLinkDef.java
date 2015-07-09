package com.zakgof.db.velvet.entity;

import java.util.List;

import com.zakgof.db.velvet.IRawVelvet.ILink;
import com.zakgof.db.velvet.IRawVelvet.LinkType;
import com.zakgof.db.velvet.IVelvet;

public class SingleLinkDef<HK, HV, CK, CV> extends ALinkDef<HK, HV, CK, CV>implements ISingleLinkDef<HK, HV, CK, CV> {

  public SingleLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
    super(hostEntity, childEntity, edgeKind);
  }

  public SingleLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    super(hostEntity, childEntity);
  }

  public static <HK, HV, CK, CV> SingleLinkDef<HK, HV, CK, CV> of(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    return new SingleLinkDef<>(hostEntity, childEntity);
  }

  public static <HK, HV, CK, CV> SingleLinkDef<HK, HV, CK, CV> of(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
    return new SingleLinkDef<>(hostEntity, childEntity, edgeKind);
  }

  @Override
  public CV single(IVelvet velvet, HV node) {
    CK bkey = singleKey(velvet, getHostEntity().keyOf(node));
    return bkey == null ? null : getChildEntity().get(velvet, bkey);
  }

  @Override
  public CK singleKey(IVelvet velvet, HK key) {
    @SuppressWarnings("unchecked")
    List<CK> linkKeys = (List<CK>) index(velvet, key).linkKeys((Class<Object>) getChildEntity().getKeyClass());
    return linkKeys.isEmpty() ? null : linkKeys.get(0);
  }

  @Override
  public void connectKeys(IVelvet velvet, Object akey, Object bkey) {
    index(velvet, akey).connect(bkey);
  }

  private ILink<Object> index(IVelvet velvet, Object akey) {
    return velvet.raw().index(akey, getKind(), LinkType.Single);
  }

  @Override
  public void disconnectKeys(IVelvet velvet, HK akey, CK bkey) {
    index(velvet, akey).disconnect(bkey);
  }

  @Override
  public boolean isConnectedKeys(IVelvet velvet, HK akey, CK bkey) {
    return index(velvet, akey).isConnected(bkey);
  }
  
  @Override
  public String toString() {
    return "single " + super.toString();
  }

}
