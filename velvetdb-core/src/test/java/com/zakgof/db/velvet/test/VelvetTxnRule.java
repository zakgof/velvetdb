package com.zakgof.db.velvet.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class VelvetTxnRule implements TestRule {

    private AVelvetTxnTest testclassinstance;

    VelvetTxnRule(AVelvetTxnTest testclassinstance) {
        this.testclassinstance = testclassinstance;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                VelvetTestSuite.velvetProvider.get().execute(velvet -> {
                    testclassinstance.velvet = velvet;
                    base.evaluate();
                });
            }
        };
    }

}
