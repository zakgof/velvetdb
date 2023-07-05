package com.zakgof.velvet.test.suite;

import com.zakgof.velvet.test.SecondaryIndexTest;
import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@ConfigurationParameter(key = "provider", value = "xodus")
// @SelectPackages("com.zakgof.velvet.test")
@SelectClasses(SecondaryIndexTest.class)
public class XodusTestSuite extends VelvetTestSuite {
}
