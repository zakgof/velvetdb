package com.zakgof.velvet.test;

import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.VelvetFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;

public class AVelvetTest {

   // private static final String PATH = "E:/Business/lmdblab/lmdb";
    // private static final String VELVET_URL = "velvetdb://lmdb/" + PATH;

    private static final String PATH = "E:/Business/lmdblab/xodus";
    private static final String VELVET_URL = "velvetdb://xodus/" + PATH;

    protected IVelvetEnvironment velvetEnv;

    @BeforeEach
    public void init() {
        File dir = new File(PATH);
        dir.mkdirs();
        for (File file : dir.listFiles())
            file.delete();
        velvetEnv = VelvetFactory.open(VELVET_URL);
    }

    @AfterEach
    public void teardown() {
        velvetEnv.close();
    }
}
