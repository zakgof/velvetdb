package com.zakgof.db.velvet.test;

import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class ParametrizedTestSuite extends Suite {

  public ParametrizedTestSuite(Class<?> klass, RunnerBuilder builder) throws InitializationError {
    super(klass, builder);
  }
  
  @Override
  protected void runChild(Runner runner, RunNotifier notifier) {
    super.runChild(runner, notifier);
  }

}
