package com.zakgof.db.velvet.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.entity.impl.Entities;
import com.zakgof.db.velvet.api.link.IMultiLinkDef;
import com.zakgof.db.velvet.api.link.ISingleLinkDef;
import com.zakgof.db.velvet.api.link.Links;


public class ConcurrentWriteTest extends AVelvetTest {

  private static final int CHUNK_SIZE = 100;
  private static final int THREADS = 5;
  
  private IEntityDef<String, TestEnt> ENTITY = Entities.anno(TestEnt.class);
  private IEntityDef<Integer, TestEnt2> ENTITY2 = Entities.anno(TestEnt2.class);

  private ISingleLinkDef<String, TestEnt, Integer, TestEnt2> SINGLE = Links.single(ENTITY, ENTITY2, "single");
  private IMultiLinkDef<String, TestEnt, Integer, TestEnt2> MULTI = Links.multi(ENTITY, ENTITY2, "multi");

  @Test
  public void testSingleTransactionMassiveEntityPut() throws InterruptedException {
    
    ExecutorService executor = Executors.newFixedThreadPool(THREADS);
    for (int t=0; t<THREADS; t++) {
      final int t1 = t;
      executor.execute(() -> {
        IVelvet v = VelvetTestSuite.velvetTxnProvider.get();
        for (int i=0; i<CHUNK_SIZE; i++) {
          System.err.println("write : " + (t1 * 1000000 + i));
          ENTITY2.put(v, new TestEnt2(t1 * 1000000 + i,  "" + t1 + "." + i));
        }
        v.commit();
      });
    }
    
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.DAYS);
   
    IVelvet v = VelvetTestSuite.velvetTxnProvider.get();
    for (int t=0; t<THREADS; t++) 
      for (int i=0; i<CHUNK_SIZE; i++) {
        System.err.println("read : " + (t * 1000000 + i));
        TestEnt2 val = ENTITY2.get(v, t * 1000000 + i);
        Assert.assertNotNull("Can't get val " + t + "." + i, val);
        Assert.assertEquals("" + t + "." + i, val.getVal());
      }
    v.commit();
    
    velvet.commit();
    
  }
 
}
