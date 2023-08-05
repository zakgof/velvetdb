package com.zakgof.velvet.test.suite;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

@Suite
@ConfigurationParameter(key = "provider", value = "xodus")
@SelectPackages("com.zakgof.velvet.test")
public class XodusTestSuite extends VelvetTestSuite {
}
