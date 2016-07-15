package com.zakgof.db.velvet.xodus;

import java.io.File;

import com.zakgof.db.velvet.VelvetFactory;

public class XodusVelvetFactory {
  
  static {
    VelvetFactory.register("xodus", queryString -> new XodusVelvetEnv(new File(queryString)));
  }

}
