package com.zakgof.db.velvet;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import com.zakgof.tools.generic.Functions;


public class VelvetFactory {
    public static IVelvetEnvironment open(String url) {
        try {
            URI u = new URI(url);
            if (!u.getScheme().equals("velvetdb"))
                throw new VelvetException("Url protocol should be velvetdb://");
            String name = u.getHost();

            ServiceLoader<IVelvetProvider> serviceLoader = ServiceLoader.load(IVelvetProvider.class);
            IVelvetProvider provider = Functions.stream(serviceLoader.iterator())
              .filter(reg -> reg.name().equals(name))
              .findFirst()
              .orElseThrow(() -> new VelvetException("Velvetdb backend not registered: " + name)); 
            return provider.open(u.getPath());
        } catch (URISyntaxException e) {
            throw new VelvetException(e);
        }
    }

    public static List<IVelvetProvider> getProviders() {
        ServiceLoader<IVelvetProvider> serviceLoader = ServiceLoader.load(IVelvetProvider.class);
        return Functions.stream(serviceLoader.iterator()).collect(Collectors.toList());
    }
}
