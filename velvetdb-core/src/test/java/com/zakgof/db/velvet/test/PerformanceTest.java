package com.zakgof.db.velvet.test;

import java.util.Random;

import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IEntityDef;

public class PerformanceTest extends AVelvetTest {

    private static final int INSERTS = 100000;
    private static final int COMMITS = 1000;
    private static final int INSERTS_PER_COMMIT = INSERTS / COMMITS;
    private IEntityDef<Integer, String> E = Entities.create(Integer.class, String.class, "kind", s -> Integer.parseInt(s.substring(1)));

    @Test
    public void testInsertSequential() {
        env.execute(velvet -> {
            for (int i = 0; i < INSERTS; i++) {
                String v = "A" + i;
                E.put(velvet, v);
            }
        });
    }

    @Test
    public void testInsertRandom() {
        env.execute(velvet -> {
            Random r = new Random(1);
            for (int i = 0; i < INSERTS; i++) {
                String v = "A" + r.nextInt();
                E.put(velvet, v);
            }
        });
    }

    @Test
    public void testOverwrite() {
        env.execute(velvet -> {
            Random r = new Random(1);
            for (int i = 0; i < INSERTS; i++) {
                String v = "A" + r.nextInt(1000);
                E.put(velvet, v);
            }
        });
    }

    @Test
    public void testInsertRandomWithCommits() {
        Random r = new Random(1);
        for (int c = 0; c < COMMITS; c++) {
            env.execute(velvet -> {
                for (int i = 0; i < INSERTS_PER_COMMIT; i++) {
                    String v = "A" + r.nextInt();
                    E.put(velvet, v);
                }
            });
        }
    }

}
