package com.zakgof.velvet.test.base;

import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.VelvetFactory;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.util.Arrays;

public class ProviderExtension implements BeforeEachCallback, AfterEachCallback {

    private IVelvetEnvironment velvetEnv;

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        String provider = context.getConfigurationParameter("provider").get();
        File dir = new File(System.getProperty("user.home"), ".velvetdb-testdb-" + provider);
        dir.mkdirs();
        for (File file : dir.listFiles())
            file.delete();
        velvetEnv = VelvetFactory.open(provider, dir.toURI().toString());
        inject(context);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        velvetEnv.close();
        velvetEnv = null;
        inject(context);
    }

    private void inject(ExtensionContext context) {
        context.getRequiredTestInstances().getAllInstances()
                .forEach(this::injectInst);
    }

    private void injectInst(Object testInstance) {
        Class<?> clazz = testInstance.getClass();
        Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(InjectedVelvetProvider.class))
                .forEach(field -> {
                    field.setAccessible(true);
                    try {
                        field.set(testInstance, velvetEnv);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
