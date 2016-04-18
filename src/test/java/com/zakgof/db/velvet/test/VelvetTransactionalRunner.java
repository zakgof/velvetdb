package com.zakgof.db.velvet.test;

import java.lang.reflect.Field;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

public class VelvetTransactionalRunner extends BlockJUnit4ClassRunner {

  public VelvetTransactionalRunner(Class<?> klass) throws InitializationError {
    super(klass);
  }
  
  @Override
  protected Statement withBefores(FrameworkMethod method, Object target, Statement statement) {
    Statement sup =  super.withBefores(method, target, statement);
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        VelvetTestSuite.velvetProvider.get().execute(velvet -> {
          Field velvetField = target.getClass().getField("velvet");
          velvetField.set(target, velvet);
          sup.evaluate();
        });
      }
    };
  }

//  @Override
//  protected Statement methodInvoker(FrameworkMethod method, Object test) {
//    Statement statement = super.methodInvoker(method, test);
//
//    return new Statement() {
//      @Override
//      public void evaluate() throws Throwable {
//        VelvetTestSuite.velvetProvider.get().execute(velvet -> {
//          Field velvetField = test.getClass().getField("velvet");
//          velvetField.set(test, velvet);
//          statement.evaluate();
//        });
//      }
//    };
//
//  }
}
