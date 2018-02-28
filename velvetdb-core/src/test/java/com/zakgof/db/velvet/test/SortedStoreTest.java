package com.zakgof.db.velvet.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.ISortableEntityDef;
import com.zakgof.db.velvet.query.IKeyQuery;
import com.zakgof.db.velvet.query.ISingleReturnKeyQuery;
import com.zakgof.db.velvet.query.KeyQueries;

public class SortedStoreTest extends AVelvetTxnTest {

    private ISortableEntityDef<Integer, TestEnt2> ENTITY2 = Entities.from(TestEnt2.class).kind("sst-testent2").makeSorted();
    private ISortableEntityDef<Integer, TestEnt3> ENTITY_EMPTY = Entities.from(TestEnt3.class).kind("realpojo").makeSorted(Integer.class, TestEnt3::getKey);

    @Before
    public void init() {
        // v1 v2 v3 v5 v7
        ENTITY2.put(velvet, new TestEnt2(7));
        ENTITY2.put(velvet, new TestEnt2(5));
        ENTITY2.put(velvet, new TestEnt2(2));
        ENTITY2.put(velvet, new TestEnt2(3));
        ENTITY2.put(velvet, new TestEnt2(1));
    }

    @After
    public void cleanup() {
        List<Integer> keys = ENTITY2.keys(velvet);
        for (Integer key : keys) {
            ENTITY2.deleteKey(velvet, key);
        }
    }

    @Test
    public void testGetAll() {
        check(KeyQueries.<Integer> builder().build(), 1, 2, 3, 5, 7);
        check(KeyQueries.<Integer> builder().descending().build(), 7, 5, 3, 2, 1);
    }

    @Test
    public void testStringOrder() {
        final ISortableEntityDef<String, TestEnt> ENTITY = Entities.from(TestEnt.class).kind("testentsorted").makeSorted();
        ENTITY.put(velvet, new TestEnt("Aaa", 7.1f));
        ENTITY.put(velvet, new TestEnt("ab", 9.1f));
        ENTITY.put(velvet, new TestEnt("Az", 6.1f));
        ENTITY.put(velvet, new TestEnt("b", 1.1f));
        ENTITY.put(velvet, new TestEnt("Aa", 5.1f));
        ENTITY.put(velvet, new TestEnt("a", 0.1f));
        ENTITY.put(velvet, new TestEnt("aaaa", 8.1f));
        ENTITY.put(velvet, new TestEnt("Aaa", 7.1f));
        ENTITY.put(velvet, new TestEnt("ab", 9.1f));
        List<String> list = ENTITY.batchGetAllList(velvet).stream().map(TestEnt::getKey).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("Aa", "Aaa", "Az", "a", "aaaa", "ab", "b"), list);
    }

    @Test
    public void testGreater() {
        check(KeyQueries.gt(0), 1, 2, 3, 5, 7);
        check(KeyQueries.gt(1), 2, 3, 5, 7);
        check(KeyQueries.gt(4), 5, 7);
        check(KeyQueries.gt(5), 7);
        check(KeyQueries.gt(7));
        check(KeyQueries.gt(8));
    }

    @Test
    public void testgeuals() {
        check(KeyQueries.ge(0), 1, 2, 3, 5, 7);
        check(KeyQueries.ge(1), 1, 2, 3, 5, 7);
        check(KeyQueries.ge(4), 5, 7);
        check(KeyQueries.ge(5), 5, 7);
        check(KeyQueries.ge(7), 7);
        check(KeyQueries.ge(8));
    }

    @Test
    public void testLess() {
        check(KeyQueries.lt(0));
        check(KeyQueries.lt(1));
        check(KeyQueries.lt(4), 1, 2, 3);
        check(KeyQueries.lt(5), 1, 2, 3);
        check(KeyQueries.lt(7), 1, 2, 3, 5);
        check(KeyQueries.lt(8), 1, 2, 3, 5, 7);
    }

    @Test
    public void testLessOrEquals() {
        check(KeyQueries.le(0));
        check(KeyQueries.le(1), 1);
        check(KeyQueries.le(4), 1, 2, 3);
        check(KeyQueries.le(5), 1, 2, 3, 5);
        check(KeyQueries.le(7), 1, 2, 3, 5, 7);
        check(KeyQueries.le(8), 1, 2, 3, 5, 7);
    }

    @Test
    public void testFirstLast() {
        check(KeyQueries.first(), 1);
        check(KeyQueries.last(), 7);
    }

    @Test
    public void testNext() {
        check(KeyQueries.next(0), 1);
        check(KeyQueries.next(1), 2);
        check(KeyQueries.next(4), 5);
        check(KeyQueries.next(5), 7);
        check(KeyQueries.next(7));
        check(KeyQueries.next(8));
    }

    @Test
    public void testPrev() {
        check(KeyQueries.prev(0));
        check(KeyQueries.prev(1));
        check(KeyQueries.prev(4), 3);
        check(KeyQueries.prev(5), 3);
        check(KeyQueries.prev(7), 5);
        check(KeyQueries.prev(8), 7);
    }

    @Test
    public void testRange() {
        check(KeyQueries.range(0, true, 8, true), 1, 2, 3, 5, 7);
        check(KeyQueries.range(0, false, 8, false), 1, 2, 3, 5, 7);
        check(KeyQueries.range(0, true, 8, true), 1, 2, 3, 5, 7);
        check(KeyQueries.range(0, true, 8, true), 1, 2, 3, 5, 7);
        check(KeyQueries.range(0, true, 8, true), 1, 2, 3, 5, 7);
        check(KeyQueries.range(0, true, 8, true), 1, 2, 3, 5, 7);
    }

    @Test
    public void testRangeDesc() {
        check(KeyQueries.<Integer> builder().descending().gt(3).build(), 7, 5);
        check(KeyQueries.<Integer> builder().descending().ge(3).build(), 7, 5, 3);
        check(KeyQueries.<Integer> builder().descending().ge(3).le(5).build(), 5, 3);
    }

    void check(ISingleReturnKeyQuery<Integer> query, Integer... ref) {
        List<TestEnt2> result = ENTITY2.get(velvet, (IKeyQuery<Integer>)query);
        Assert.assertEquals(Arrays.stream(ref).collect(Collectors.toList()), result.stream().map(TestEnt2::getKey).collect(Collectors.toList()));
        List<TestEnt3> resultE = ENTITY_EMPTY.get(velvet, (IKeyQuery<Integer>)query);
        Assert.assertEquals(Collections.emptyList(), resultE);
    }

}
