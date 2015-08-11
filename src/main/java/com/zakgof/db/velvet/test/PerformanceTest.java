package com.zakgof.db.velvet.test;

import java.util.Random;

import org.junit.Test;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.entity.impl.Entities;

public class PerformanceTest {

  private static final int INSERTS = 100000;
  private static final int COMMITS = 500;
  private static final int INSERTS_PER_COMMIT = INSERTS / COMMITS;
  private IVelvet velvet;
  private IEntityDef<Integer, String> E = Entities.create(Integer.class, String.class, "kind", s -> Integer.parseInt(s.substring(1)));
  
  public PerformanceTest() {
    velvet = VelvetTestSuite.velvetProvider.get();
  }

  @Test
  public void testInsertSequential() {
    for (int i = 0; i < INSERTS; i++) {
      String v = "A" + i;
      E.put(velvet, v);
    }
    velvet.commit();
  }

  @Test
  public void testInsertRandom() {
    Random r = new Random(1);
    for (int i = 0; i < INSERTS; i++) {
      String v = "A" + r.nextInt();
      E.put(velvet, v);
    }
    velvet.commit();
  }

  @Test
  public void testOverwrite() {
    Random r = new Random(1);
    for (int i = 0; i < INSERTS; i++) {
      String v = "A" + r.nextInt(1000);
      E.put(velvet, v);
    }
    velvet.commit();
  }

  @Test
  public void testInsertRandomWithCommits() {
    Random r = new Random(1);
    for (int c = 0; c < COMMITS; c++) {
      for (int i = 0; i < INSERTS_PER_COMMIT; i++) {
        String v = "A" + r.nextInt();
        E.put(velvet, v);
      }
      velvet.commit();
    }
  }

}
