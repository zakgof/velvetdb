package com.zakgof.velvet.test.suite;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectPackages("com.zakgof.velvet.test")
@ConfigurationParameter(key = "provider", value = "xodus")
@ConfigurationParameter(key = "url", value = "/")
public class VelvetTestSuite {
}
