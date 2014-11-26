package com.zakgof.db.velvet;


public class AdminUtil {
  
  /*

  public static void copy(IVelvet dest, IVelvet source, ZeDataModel model) {
    copyNodes(dest, source, model);
    copyLinks(dest, source, model);
  }

  private static void copyNodes(IVelvet dest, IVelvet source, ZeDataModel model) {
    for (String entity : model.entityNames()) {      
      Class<?> entityClass = model.getEntity(entity);
      List<?> nodes = source.allOf(entityClass);
      System.err.print(entity + " " + nodes.size() + " nodes... ");
      for (Object key : nodes) {
        if (VelvetUtil.keyOf(key) != null)
          dest.put(key);
      }
      System.err.println(" done.");
    }
  }

  private static void copyLinks(IVelvet dest, IVelvet source, ZeDataModel model) {
    for (String entity : model.entityNames()) {
      Class<?> entityClass = model.getEntity(entity);
      Collection<?> keys = source.raw().allKeys(entity, entityClass);

      ILinkProvider linkProvider = model.getLinks(entityClass);
      
      for (ILinkDef<?, ?> link : linkProvider.declaredLinks()) {
        
        if (link.getHostClass() != entityClass)
          continue;
        
        if (link instanceof ISingleLinkDef<?, ?>)
          copySingleLinks(dest, source, keys, (ISingleLinkDef<?, ?>) link);
        else
          copyMultiLinks(dest, source, keys, (IMultiLinkDef<?, ?>) link);
      }
    }

  }

  private static void copySingleLinks(IVelvet dest, IVelvet source, Collection<?> keys, ISingleLinkDef<?, ?> single) {
    System.err.print(single + " " + keys.size() + " origins... ");
    for (Object originKey : keys) {
      if (originKey == null)
        continue;
      Object destKey = single.singleKey(source, originKey);
      if (destKey != null) {
        System.err.print(" " + originKey + "->" + destKey + ", ");
        single.connectKeys(dest, originKey, destKey);
      }
    }
    System.err.println(" done.");
  }

  private static void copyMultiLinks(IVelvet dest, IVelvet source, Collection<?> keys, IMultiLinkDef<?, ?> multi) {
    System.err.print(multi + " " + keys.size() + " origins... ");
    for (Object originKey : keys) {
      List<Object> destKeys = multi.linkKeys(source, originKey);
      for (Object destKey : destKeys)
        multi.connectKeys(dest, originKey, destKey);
    }
    System.err.println(" done.");
  }
  */

}
