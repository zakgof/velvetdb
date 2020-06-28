package com.zakgof.db.velvet.test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.ISortableEntityDef;
import com.zakgof.db.velvet.query.IKeyQuery;
import com.zakgof.db.velvet.query.ISecQuery;
import com.zakgof.db.velvet.query.KeyQueries;
import com.zakgof.db.velvet.query.SecQueries;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SortedStoreIndexesTest extends AVelvetTxnTest {

    private ISortableEntityDef<Integer, TestEnt3> ENTITY3 = Entities.from(TestEnt3.class).kind("sortedindexed3").index("str", TestEnt3::getStr, String.class).index("weight", TestEnt3::getWeight, long.class).makeSorted(Integer.class, TestEnt3::getKey);

    private TestEnt3 value7;

    @Before
    public void init() {
        ENTITY3.put(velvet, new TestEnt3(1, 100L, "l"));
        ENTITY3.put(velvet, new TestEnt3(2, 1000L, "i"));
        ENTITY3.put(velvet, new TestEnt3(3, 900L, "m"));
        ENTITY3.put(velvet, new TestEnt3(4, 800L, "h"));
        ENTITY3.put(velvet, new TestEnt3(5, 300L, null));
        ENTITY3.put(velvet, new TestEnt3(6, 1200L, "f"));
        value7 = new TestEnt3(7, 400L, "c");
        ENTITY3.put(velvet, value7);
        ENTITY3.put(velvet, new TestEnt3(8, 1100L, "k"));
        ENTITY3.put(velvet, new TestEnt3(9, 200L, "g"));
        ENTITY3.put(velvet, new TestEnt3(10, 600L, "a"));
        ENTITY3.put(velvet, new TestEnt3(11, 700L, "b"));
        ENTITY3.put(velvet, new TestEnt3(12, 1300L, "d"));
        ENTITY3.put(velvet, new TestEnt3(13, 500L, "j"));
    }

    @Test
    public void testGetAll() {
        checkPri(KeyQueries.<Integer> builder().build(), "limh*fckgabdj");
        check("weight", SecQueries.<Integer, Integer> builder().build(), "lg*cjabhmikfd");
        check("str", SecQueries.<Integer, Integer> builder().build(), "*abcdfghijklm");
    }

    @Test
    public void testRange() {
        checkPri(KeyQueries.<Integer> range(4, false, 10, false), "*fckg");
        check("weight", SecQueries.<Integer, Long> range(250L, false, 1101L, true), "*cjabhmik");
        check("str", SecQueries.<Integer, String> range("c", true, "i", false), "cdfgh");
    }

    @Test
    public void testOpenRange() {
        checkPri(KeyQueries.lt(8), "limh*fc");
        check("weight", SecQueries.ge(600L), "abhmikfd");
        check("str", SecQueries.le("g"), "*abcdfg");
    }

    @Test
    public void testOpenRangeDesc() {
        checkPri(KeyQueries.<Integer> builder().lt(8).descending().build(), "cf*hmil");
        check("weight", SecQueries.<Integer, Long> builder().ge(600L).descending().build(), "dfkimhba");
        check("str", SecQueries.<Integer, String> builder().le("g").descending().build(), "gfdcba*");
    }

    @Test
    public void testKeyOpenRange() {
        checkPri(KeyQueries.<Integer> builder().lt(8).build(), "limh*fc");
        check("weight", SecQueries.<Integer, Integer> builder().geKey(8).build(), "kfd");
        check("str", SecQueries.<Integer, Integer> builder().leKey(8).build(), "*abcdfghijk");
    }

    @Test
    public void testKeyOpenRangeDesc() {
        checkPri(KeyQueries.<Integer> builder().lt(8).descending().build(), "cf*hmil");
        check("weight", SecQueries.<Integer, Long> builder().geKey(8).descending().build(), "dfk");
        check("str", SecQueries.<Integer, String> builder().leKey(8).descending().build(), "kjihgfdcba*");
    }

    @Test
    public void testRemove() {
        ENTITY3.deleteValue(velvet, value7);
        checkPri(KeyQueries.<Integer> builder().build(), "limh*fkgabdj");
        check("weight", SecQueries.<Integer, Integer> builder().build(), "lg*jabhmikfd");
        check("str", SecQueries.<Integer, Integer> builder().build(), "*abdfghijklm");
    }

    private <K, M extends Comparable<? super M>> void check(String name, ISecQuery<Integer, M> query, String ref) {
        List<Integer> keys = ENTITY3.<M> queryKeys(velvet, name, query);
        checkResult(ref, keys);
    }

    private <K, M extends Comparable<? super M>> void checkPri(IKeyQuery<Integer> query, String ref) {
        List<Integer> keys = ENTITY3.queryKeys(velvet, query);
        checkResult(ref, keys);
    }

    private void checkResult(String ref, List<Integer> keys) {
        List<TestEnt3> values = ENTITY3.batchGetList(velvet, keys);
        String result = values.stream()
                .map(TestEnt3::getStr)
                .map(str -> str == null ? "*" : str)
                .collect(Collectors.joining(""));
        Assert.assertEquals(ref, result);
    }


}
