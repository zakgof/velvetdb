package com.zakgof.db.velvet.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IMultiLinkDef;
import com.zakgof.db.velvet.link.ISingleLinkDef;
import com.zakgof.db.velvet.link.Links;


public class ConcurrentWriteTest extends AVelvetTest {

  private static final int CHUNK_SIZE = 10000;
  private static final int THREADS = 50;
  private static final int TXN_PER_THREAD = 10;
  
  private IEntityDef<String, TestEnt> ENTITY = Entities.create(TestEnt.class);
  private IEntityDef<Integer, TestEnt2> ENTITY2 = Entities.create(TestEnt2.class);

  private ISingleLinkDef<String, TestEnt, Integer, TestEnt2> SINGLE = Links.single(ENTITY, ENTITY2, "single");
  private IMultiLinkDef<String, TestEnt, Integer, TestEnt2> MULTI = Links.multi(ENTITY, ENTITY2, "multi");

  @Test
  public void testSingleTransactionMassiveEntityPut() throws InterruptedException {
    
    ExecutorService executor = Executors.newFixedThreadPool(THREADS);
    for (int t=0; t<THREADS; t++) {
      final int t1 = t;
      executor.execute(() -> {
        env.execute(velvet ->  {
          for (int i=0; i<CHUNK_SIZE; i++) {
            ENTITY2.put(velvet, new TestEnt2(t1 * 1000000 + i,  "" + t1 + "." + i));
          }
        });        
      });
    }
    
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.DAYS);
   
    checkValues();
  }
  
  @Test
  public void testMultipleTransactionMassiveEntityPut() throws InterruptedException {
    
    ExecutorService executor = Executors.newFixedThreadPool(THREADS);
    for (int t=0; t<THREADS; t++) {
      final int t1 = t;
      executor.execute(() -> {
        for (int x=0; x<TXN_PER_THREAD; x++ ) {
          final int x1 = x;
          env.execute(velvet ->  {
            for (int i=0; i<CHUNK_SIZE / TXN_PER_THREAD; i++) {
              int index =  x1 * CHUNK_SIZE / TXN_PER_THREAD + i;
              ENTITY2.put(velvet, new TestEnt2(t1 * 1000000 + index,  "" + t1 + "." + index));
            }
          });    
        }
      });
    }
    
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.DAYS);
   
    checkValues();
  }
  
  @Test
  public void testMultipleTransactionMassiveLinkConnect() throws InterruptedException {
    
   // TODO
  }
  
  

  private void checkValues() {
    env.execute(velvet ->  {
    for (int t=0; t<THREADS; t++) 
      for (int i=0; i<CHUNK_SIZE; i++) {
        TestEnt2 val = ENTITY2.get(velvet, t * 1000000 + i);
        Assert.assertNotNull("Can't get val " + t + "." + i, val);
        Assert.assertEquals("" + t + "." + i, val.getVal());
      }
    });
  }
 
}
