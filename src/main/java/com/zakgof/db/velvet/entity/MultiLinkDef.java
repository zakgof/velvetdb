package com.zakgof.db.velvet.entity;

import java.util.List;

import com.zakgof.db.velvet.IRawVelvet.ILink;
import com.zakgof.db.velvet.IRawVelvet.LinkType;
import com.zakgof.db.velvet.IVelvet;

public class MultiLinkDef<HK, HV, CK, CV> extends ALinkDef<HK, HV, CK, CV>implements IMultiLinkDef<HK, HV, CK, CV> {

  public MultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
    super(hostEntity, childEntity, edgeKind);
  }

  public MultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    super(hostEntity, childEntity);
  }

  public static <HK, HV, CK, CV> MultiLinkDef<HK, HV, CK, CV> of(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    return new MultiLinkDef<>(hostEntity, childEntity);
  }

  public static <HK, HV, CK, CV> MultiLinkDef<HK, HV, CK, CV> of(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
    return new MultiLinkDef<>(hostEntity, childEntity, edgeKind);
  }

  @Override
  public List<CV> multi(IVelvet velvet, HV node) {
    return getChildEntity().getAll(velvet, multiKeys(velvet, getHostEntity().keyOf(node)));
  }

  public List<CK> multiKeys(IVelvet velvet, HK key) {
    return this.<CK>index(velvet, key).linkKeys(getChildEntity().getKeyClass());
  }

  @Override
  public String toString() {
    return "multi " + super.toString();
  }

  @Override
  public void connectKeys(IVelvet velvet, Object akey, Object bkey) {
    index(velvet, akey).connect(bkey); 
  }

  @Override
  public void disconnectKeys(IVelvet velvet, Object akey, Object bkey) {
    index(velvet, akey).disconnect(bkey);    
  }
  
  @Override
  public boolean isConnectedKeys(IVelvet velvet, Object akey, Object bkey) {
    return index(velvet, akey).isConnected(bkey); 
  }

  private <K> ILink<K> index(IVelvet velvet, Object akey) {
    return velvet.raw().index(akey, getKind(), LinkType.Multi);
  }

  
}
