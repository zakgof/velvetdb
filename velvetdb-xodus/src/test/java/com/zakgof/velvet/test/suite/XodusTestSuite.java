package com.zakgof.velvet.test.suite;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.Suite;

@Suite
@ConfigurationParameter(key = "provider", value = "xodus")
public class XodusTestSuite extends VelvetTestSuite {
}
