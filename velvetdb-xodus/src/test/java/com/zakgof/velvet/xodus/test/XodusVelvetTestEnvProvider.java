package com.zakgof.velvet.xodus.test;

import com.zakgof.velvet.VelvetFactory;
import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.test.ITestVelvetEnvProvider;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class XodusVelvetTestEnvProvider implements ITestVelvetEnvProvider {

    private String path;

    @Override
    public IVelvetEnvironment create() {
        try {
            path = Files.createTempDirectory("velvet").toString();
            new File(path).mkdirs();
            return VelvetFactory.open("velvetdb://xodus/" + path.replace(File.separatorChar, '/'));
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void close() {
        for (File file : new File(path).listFiles()) {
            file.delete();
        }
    }
}
