package com.zakgof.db.velvet;

public class VelvetUtil {

//  public static <A, B> void upgrade(IVelvet velvet, Class<A> oldClass, Class<B> newClass, IFunction<A, B> convertor) {
//    String kind = kindFromClass(newClass);
//    if (!kind.equals(kindFromClass(oldClass)))
//      throw new RuntimeException("Upgrage impossible: kinds not match");
//    List<A> oldValues = velvet.allOf(oldClass);
//    for (A oldValue : oldValues) {
//      B newValue = convertor.get(oldValue);
//      if (!keyOfValue(newValue).equals(keyOfValue(oldValue)))
//        throw new RuntimeException("Upgrage error: keys not match");
//      velvet.put(newValue);
//    }
//  }
//
//  public static boolean isAutoKeyed(Class<?> clazz) {
//    try {
//      for (Field field : getAllFields(clazz)) {
//        field.setAccessible(true);
//        if (field.getAnnotation(AutoKey.class) != null)
//          return true;
//      }
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//    return false;
//  }
//
//  public static boolean isEntity(Object node) {
//    try {
//      keyOfValue(node);
//    } catch (Throwable e) {
//      return false;
//    }
//    return true;
//  }

}
