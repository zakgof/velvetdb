package com.zakgof.db.velvet;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;

public class VelvetFactory {
  
  private static ConcurrentHashMap<String, IVelvetInitializer> backends = new ConcurrentHashMap<>();

  public static IVelvetEnvironment open(String url) {
    try {
      URI u = new URI(url);
      if (!u.getScheme().equals("velvetdb"))
        throw new VelvetException("Invalid url protocol");
      
      String name = u.getHost();
      IVelvetInitializer initializer = backends.get(name);
      if (initializer == null)
        throw new VelvetException("Velvetdb backend not registered: " + name); 

      return initializer.open(u.getPath());
      
    } catch (URISyntaxException e) {
      throw new VelvetException(e);
    }
  }

  public static void register(String name, IVelvetInitializer initializer) {
    backends.put(name, initializer);
  }

}
