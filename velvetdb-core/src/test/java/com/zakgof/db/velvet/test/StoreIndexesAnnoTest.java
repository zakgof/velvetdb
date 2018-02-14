package com.zakgof.db.velvet.test;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.Queries;

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
        check("key", Queries.<Integer, Integer> builder().build(), "limhefckgabdj");
        check("w2", Queries.<Integer, Integer> builder().build(), "lgecjabhmikfd");
        check("str", Queries.<Integer, Integer> builder().build(), "abcdefghijklm");
    }

    @Test
    public void testRange() {
        check("key", Queries.<Integer, Integer> range(4, false, 10, false), "efckg");
        check("w2", Queries.<Integer, Long> range(250L, false, 1101L, true), "ecjabhmik");
        check("str", Queries.<Integer, String> range("c", true, "i", false), "cdefgh");
    }

    @Test
    public void testOpenRange() {
        check("key", Queries.less(8), "limhefc");
        check("w2", Queries.greaterOrEq(600L), "abhmikfd");
        check("str", Queries.lessOrEq("g"), "abcdefg");
    }

    @Test
    public void testOpenRangeDesc() {
        check("key", Queries.<Integer, Integer>builder().less(8).descending().build(), "cfehmil");
        check("w2", Queries.<Integer, Long>builder().greaterOrEq(600L).descending().build(), "dfkimhba");
        check("str", Queries.<Integer, String> builder().lessOrEq("g").descending().build(), "gfedcba");
    }

    @Test
    public void testKeyOpenRange() {
        check("key", Queries.<Integer, Integer>builder().lessKey(8).build(), "limhefc");
        check("w2", Queries.<Integer, Integer>builder().greaterOrEqKey(8).build(), "kfd");
        check("str", Queries.<Integer, Integer>builder().lessOrEqKey(8).build(), "abcdefghijk");
    }

    @Test
    public void testKeyOpenRangeDesc() {
        check("key", Queries.<Integer, Integer>builder().lessKey(8).descending().build(), "cfehmil");
        check("w2", Queries.<Integer, Long>builder().greaterOrEqKey(8).descending().build(), "dfk");
        check("str", Queries.<Integer, String> builder().lessOrEqKey(8).descending().build(), "kjihgfedcba");
    }

    @Test
    public void testRemove() {
        ENTITY3.deleteValue(velvet, value7);
        check("key", Queries.<Integer, Integer> builder().build(), "limhefkgabdj");
        check("w2", Queries.<Integer, Integer> builder().build(), "lgejabhmikfd");
        check("str", Queries.<Integer, Integer> builder().build(), "abdefghijklm");
    }

    private <K, M extends Comparable<? super M>> void check(String name, IRangeQuery<Integer, M> query, String ref) {
        List<Integer> keys = ENTITY3.<M> indexKeys(velvet, name, query);
        List<TestEntInd> values = ENTITY3.get(velvet, keys);
        String result = values.stream().map(TestEntInd::getStr).collect(Collectors.joining(""));
        Assert.assertEquals(ref, result);
    }

}
