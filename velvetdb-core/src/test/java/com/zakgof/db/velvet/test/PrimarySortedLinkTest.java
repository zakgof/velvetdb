package com.zakgof.db.velvet.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.impl.link.PriIndexMultiLinkDef;
import com.zakgof.db.velvet.link.Links;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.Queries;

public class PrimarySortedLinkTest extends AVelvetTxnTest {

    protected IEntityDef<String, TestEnt> ENTITY = Entities.create(TestEnt.class);
    protected IEntityDef<Integer, TestEnt2> ENTITY2 = Entities.create(TestEnt2.class);
    protected PriIndexMultiLinkDef<String, TestEnt, Integer, TestEnt2> MULTI = Links.pri(ENTITY, ENTITY2);

    private TestEnt root;

    @Before
    public void init() {

        Integer[] keys = new Integer[] { 7, 2, 9, 1, 4, 8, 3, 6 };
        root = new TestEnt("root", 1.0f);
        ENTITY.put(velvet, root);

        for (Integer key : keys) {
            TestEnt2 e2 = new TestEnt2(key);
            ENTITY2.put(velvet, e2);
            MULTI.connect(velvet, root, e2);
        }

    }

    @Test
    public void testGreaterOrEq() {
        check(Queries.greaterOrEq(-1), 1, 2, 3, 4, 6, 7, 8, 9);
        check(Queries.greaterOrEq(1), 1, 2, 3, 4, 6, 7, 8, 9);
        check(Queries.greaterOrEq(5), 6, 7, 8, 9);
        check(Queries.greaterOrEq(6), 6, 7, 8, 9);
        check(Queries.greaterOrEq(9), 9);
        check(Queries.greaterOrEq(10));
    }

    @Test
    public void testGreaterOrEqDesc() {
        check(Queries.<Integer, Integer> builder().descending().greaterOrEq(-1).build(), 9, 8, 7, 6, 4, 3, 2, 1);
        check(Queries.<Integer, Integer> builder().descending().greaterOrEq(1).build(), 9, 8, 7, 6, 4, 3, 2, 1);
        check(Queries.<Integer, Integer> builder().descending().greaterOrEq(5).build(), 9, 8, 7, 6);
        check(Queries.<Integer, Integer> builder().descending().greaterOrEq(6).build(), 9, 8, 7, 6);
        check(Queries.<Integer, Integer> builder().descending().greaterOrEq(9).build(), 9);
        check(Queries.<Integer, Integer> builder().descending().greaterOrEq(10).build());
    }

    @Test
    public void testEqualsTo() {
        check(Queries.equalsTo(4), 4);
        check(Queries.equalsTo(1), 1);
        check(Queries.equalsTo(9), 9);
        check(Queries.equalsTo(-1));
        check(Queries.equalsTo(5));
        check(Queries.equalsTo(10));
    }

    @Test
    public void testGreater() {
        check(Queries.greater(-1), 1, 2, 3, 4, 6, 7, 8, 9);
        check(Queries.greater(1), 2, 3, 4, 6, 7, 8, 9);
        check(Queries.greater(5), 6, 7, 8, 9);
        check(Queries.greater(6), 7, 8, 9);
        check(Queries.greater(9));
        check(Queries.greater(10));
    }

    @Test
    public void testFirstLast() {
        check(Queries.first(), 1);
        check(Queries.last(), 9);
    }

    @Test
    public void testNext() {
        check(Queries.next(-1), 1);
        check(Queries.next(1), 2);
        check(Queries.next(4), 6);
        check(Queries.next(9));
    }

    @Test
    public void testLess() {
        check(Queries.less(-1));
        check(Queries.less(1));
        check(Queries.less(2), 1);
        check(Queries.less(5), 1, 2, 3, 4);
        check(Queries.less(6), 1, 2, 3, 4);
        check(Queries.less(9), 1, 2, 3, 4, 6, 7, 8);
        check(Queries.less(10), 1, 2, 3, 4, 6, 7, 8, 9);
    }

    @Test
    public void testLessOrEq() {
        check(Queries.lessOrEq(-1));
        check(Queries.lessOrEq(1), 1);
        check(Queries.lessOrEq(2), 1, 2);
        check(Queries.lessOrEq(5), 1, 2, 3, 4);
        check(Queries.lessOrEq(6), 1, 2, 3, 4, 6);
        check(Queries.lessOrEq(9), 1, 2, 3, 4, 6, 7, 8, 9);
        check(Queries.lessOrEq(10), 1, 2, 3, 4, 6, 7, 8, 9);
    }

    @Test
    public void testRange() {
        check(Queries.range(-1, true, 10, true), 1, 2, 3, 4, 6, 7, 8, 9);
        check(Queries.range(-1, false, 10, false), 1, 2, 3, 4, 6, 7, 8, 9);
        check(Queries.range(0, true, 9, false), 1, 2, 3, 4, 6, 7, 8);
        check(Queries.range(1, false, 9, true), 2, 3, 4, 6, 7, 8, 9);
        check(Queries.range(2, true, 7, false), 2, 3, 4, 6);
        check(Queries.range(2, true, 6, true), 2, 3, 4, 6);
        check(Queries.range(5, true, 6, false));
        check(Queries.range(6, true, 6, true), 6);
        check(Queries.range(6, true, 6, false));
        check(Queries.range(6, false, 6, true));
        check(Queries.range(5, true, 2, true));
    }

    @Test
    public void testRangeDesc() {
        check(Queries.<Integer, Integer> builder().descending().greaterOrEq(2).less(8).build(), 7, 6, 4, 3, 2);
        check(Queries.<Integer, Integer> builder().descending().greaterOrEq(8).less(8).build());
        check(Queries.<Integer, Integer> builder().descending().greater(0).less(3).build(), 2, 1);
        check(Queries.<Integer, Integer> builder().lessOrEq(7).descending().greater(5).build(), 7, 6);
        check(Queries.<Integer, Integer> builder().less(10).descending().greaterOrEq(5).build(), 9, 8, 7, 6);
        check(Queries.<Integer, Integer> builder().lessOrEq(10).descending().greaterOrEq(5).build(), 9, 8, 7, 6);
        check(Queries.<Integer, Integer> builder().less(9).descending().greaterOrEq(5).build(), 8, 7, 6);
        check(Queries.<Integer, Integer> builder().lessOrEq(9).descending().greaterOrEq(5).build(), 9, 8, 7, 6);
    }

    @Test
    public void testLimitOffset() {
        check(Queries.<Integer, Integer> builder().greaterOrEq(2).less(8).limit(2).build(), 2, 3);
        check(Queries.<Integer, Integer> builder().greaterOrEq(2).less(8).limit(10).build(), 2, 3, 4, 6, 7);
        check(Queries.<Integer, Integer> builder().greaterOrEq(2).less(8).limit(10).offset(2).build(), 4, 6, 7);
        check(Queries.<Integer, Integer> builder().greaterOrEq(2).less(8).limit(2).offset(2).build(), 4, 6);
        check(Queries.<Integer, Integer> builder().greaterOrEq(2).less(8).limit(1).offset(10).build());
        check(Queries.<Integer, Integer> builder().greaterOrEq(2).less(8).limit(1).descending().offset(1).build(), 6);
        check(Queries.<Integer, Integer> builder().greaterOrEq(2).less(8).limit(10).descending().offset(1).build(), 6, 4, 3, 2);
        check(Queries.<Integer, Integer> builder().greaterOrEq(2).less(8).limit(1).descending().offset(4).build(), 2);
        check(Queries.<Integer, Integer> builder().limit(4).build(), 1, 2, 3, 4);
        check(Queries.<Integer, Integer> builder().limit(4).descending().build(), 9, 8, 7, 6);
        check(Queries.<Integer, Integer> builder().limit(4).offset(2).build(), 3, 4, 6, 7);
        check(Queries.<Integer, Integer> builder().limit(4).descending().offset(5).build(), 3, 2, 1);
    }

    @Test
    public void testDelete() {
        check(Queries.<Integer, Integer> builder().build(), 1, 2, 3, 4, 6, 7, 8, 9);
        MULTI.disconnectKeys(velvet, root.getKey(), 3);
        check(Queries.<Integer, Integer> builder().build(), 1, 2, 4, 6, 7, 8, 9);
        MULTI.disconnectKeys(velvet, root.getKey(), 1);
        check(Queries.<Integer, Integer> builder().build(), 2, 4, 6, 7, 8, 9);
        MULTI.disconnectKeys(velvet, root.getKey(), 9);
        check(Queries.<Integer, Integer> builder().build(), 2, 4, 6, 7, 8);
        MULTI.disconnectKeys(velvet, root.getKey(), 4);
        check(Queries.<Integer, Integer> builder().build(), 2, 6, 7, 8);
        MULTI.disconnectKeys(velvet, root.getKey(), 1); // NOOP
        check(Queries.<Integer, Integer> builder().build(), 2, 6, 7, 8);
        MULTI.disconnectKeys(velvet, root.getKey(), 7);
        check(Queries.<Integer, Integer> builder().build(), 2, 6, 8);
        MULTI.disconnectKeys(velvet, root.getKey(), 2);
        check(Queries.<Integer, Integer> builder().build(), 6, 8);
        MULTI.disconnectKeys(velvet, root.getKey(), 8);
        check(Queries.<Integer, Integer> builder().build(), 6);
        MULTI.disconnectKeys(velvet, root.getKey(), 6);
        check(Queries.<Integer, Integer> builder().build());
    }

    @Test
    public void testAllKeys() {
        int[] keys = MULTI.multi(velvet, root).stream().mapToInt(TestEnt2::getKey).toArray();
        Assert.assertArrayEquals(new int[] { 1, 2, 3, 4, 6, 7, 8, 9 }, keys);
    }

    @Test
    public void testIsConnected() {
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 1));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 2));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 3));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 4));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 6));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 7));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 8));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 9));

        Assert.assertFalse(MULTI.isConnectedKeys(velvet, root.getKey(), 0));
        Assert.assertFalse(MULTI.isConnectedKeys(velvet, root.getKey(), 5));
        Assert.assertFalse(MULTI.isConnectedKeys(velvet, root.getKey(), -1));
        Assert.assertFalse(MULTI.isConnectedKeys(velvet, root.getKey(), 10));
        Assert.assertFalse(MULTI.isConnectedKeys(velvet, root.getKey(), 11));
    }

    private void check(IRangeQuery<Integer, Integer> query, int... v) {
        int[] keys = MULTI.indexed(query).multi(velvet, root).stream().mapToInt(TestEnt2::getKey).toArray();
        Assert.assertArrayEquals(v, keys);
    }

}
