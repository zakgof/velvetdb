package com.zakgof.db.velvet;

public interface IVelvetProvider {

  IVelvetEnvironment open(String path);

  String name();
}
