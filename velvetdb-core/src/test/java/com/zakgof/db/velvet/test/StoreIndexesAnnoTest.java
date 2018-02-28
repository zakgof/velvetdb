package com.zakgof.db.velvet.test;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.query.ISecQuery;
import com.zakgof.db.velvet.query.SecQueries;

public class StoreIndexesAnnoTest extends AVelvetTxnTest {

    private IEntityDef<Integer, TestEntInd> ENTITY3 = Entities.from(TestEntInd.class)
         .kind("indexed-anno")
         .make();

    private TestEntInd value7;

    @Before
    public void init() {
        ENTITY3.put(velvet, new TestEntInd(1, 100L, "l"));
        ENTITY3.put(velvet, new TestEntInd(2, 1000L, "i"));
        ENTITY3.put(velvet, new TestEntInd(3, 900L, "m"));
        ENTITY3.put(velvet, new TestEntInd(4, 800L, "h"));
        ENTITY3.put(velvet, new TestEntInd(5, 300L, "e"));
        ENTITY3.put(velvet, new TestEntInd(6, 1200L, "f"));
        value7 = new TestEntInd(7, 400L, "c");
        ENTITY3.put(velvet, value7);
        ENTITY3.put(velvet, new TestEntInd(8, 1100L, "k"));
        ENTITY3.put(velvet, new TestEntInd(9, 200L, "g"));
        ENTITY3.put(velvet, new TestEntInd(10, 600L, "a"));
        ENTITY3.put(velvet, new TestEntInd(11, 700L, "b"));
        ENTITY3.put(velvet, new TestEntInd(12, 1300L, "d"));
        ENTITY3.put(velvet, new TestEntInd(13, 500L, "j"));
    }

    @Test
    public void testGetAll() {
        check("key", SecQueries.<Integer, Integer> builder().build(), "limhefckgabdj");
        check("w2", SecQueries.<Integer, Integer> builder().build(), "lgecjabhmikfd");
        check("str", SecQueries.<Integer, Integer> builder().build(), "abcdefghijklm");
    }

    @Test
    public void testRange() {
        check("key", SecQueries.<Integer, Integer> range(4, false, 10, false), "efckg");
        check("w2", SecQueries.<Integer, Long> range(250L, false, 1101L, true), "ecjabhmik");
        check("str", SecQueries.<Integer, String> range("c", true, "i", false), "cdefgh");
    }

    @Test
    public void testOpenRange() {
        check("key", SecQueries.lt(8), "limhefc");
        check("w2", SecQueries.ge(600L), "abhmikfd");
        check("str", SecQueries.le("g"), "abcdefg");
    }

    @Test
    public void testOpenRangeDesc() {
        check("key", SecQueries.<Integer, Integer>builder().lt(8).descending().build(), "cfehmil");
        check("w2", SecQueries.<Integer, Long>builder().ge(600L).descending().build(), "dfkimhba");
        check("str", SecQueries.<Integer, String> builder().le("g").descending().build(), "gfedcba");
    }

    @Test
    public void testKeyOpenRange() {
        check("key", SecQueries.<Integer, Integer>builder().ltKey(8).build(), "limhefc");
        check("w2", SecQueries.<Integer, Integer>builder().geKey(8).build(), "kfd");
        check("str", SecQueries.<Integer, Integer>builder().leKey(8).build(), "abcdefghijk");
    }

    @Test
    public void testKeyOpenRangeDesc() {
        check("key", SecQueries.<Integer, Integer>builder().ltKey(8).descending().build(), "cfehmil");
        check("w2", SecQueries.<Integer, Long>builder().geKey(8).descending().build(), "dfk");
        check("str", SecQueries.<Integer, String> builder().leKey(8).descending().build(), "kjihgfedcba");
    }

    @Test
    public void testRemove() {
        ENTITY3.deleteValue(velvet, value7);
        check("key", SecQueries.<Integer, Integer> builder().build(), "limhefkgabdj");
        check("w2", SecQueries.<Integer, Integer> builder().build(), "lgejabhmikfd");
        check("str", SecQueries.<Integer, Integer> builder().build(), "abdefghijklm");
    }

    private <K, M extends Comparable<? super M>> void check(String name, ISecQuery<Integer, M> query, String ref) {
        List<Integer> keys = ENTITY3.<M> indexKeys(velvet, name, query);
        List<TestEntInd> values = ENTITY3.batchGetList(velvet, keys);
        String result = values.stream().map(TestEntInd::getStr).collect(Collectors.joining(""));
        Assert.assertEquals(ref, result);
    }

}
