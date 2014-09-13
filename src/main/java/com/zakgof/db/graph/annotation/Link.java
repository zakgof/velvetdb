package com.zakgof.db.graph.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Link {
  
  public enum GetMode {
    IGNORE,
    FETCH
  }
  
  public enum PutMode {
    IGNORE,
    LINK,
    BILINK
  }
  
  GetMode get() default GetMode.FETCH;
  
  PutMode put() default PutMode.LINK;
  
  String edgeKind() default "";
  
  String backEdgeKind() default "";

}
