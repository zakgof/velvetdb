package com.zakgof.db.velvet.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IPriMultiLinkDef;
import com.zakgof.db.velvet.link.Links;
import com.zakgof.db.velvet.query.IKeyQuery;
import com.zakgof.db.velvet.query.ISingleReturnKeyQuery;
import com.zakgof.db.velvet.query.KeyQueries;

public class PrimarySortedLinkTest extends AVelvetTxnTest {

    protected IEntityDef<String, TestEnt> ENTITY = Entities.create(TestEnt.class);
    protected IEntityDef<Integer, TestEnt2> ENTITY2 = Entities.create(TestEnt2.class);
    protected IPriMultiLinkDef<String, TestEnt, Integer, TestEnt2> MULTI = Links.pri(ENTITY, ENTITY2);

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
        check(KeyQueries.ge(-1), 1, 2, 3, 4, 6, 7, 8, 9);
        check(KeyQueries.ge(1), 1, 2, 3, 4, 6, 7, 8, 9);
        check(KeyQueries.ge(5), 6, 7, 8, 9);
        check(KeyQueries.ge(6), 6, 7, 8, 9);
        check(KeyQueries.ge(9), 9);
        check(KeyQueries.ge(10));
    }

    @Test
    public void testGreaterOrEqDesc() {
        check(KeyQueries.<Integer> builder().descending().ge(-1).build(), 9, 8, 7, 6, 4, 3, 2, 1);
        check(KeyQueries.<Integer> builder().descending().ge(1).build(), 9, 8, 7, 6, 4, 3, 2, 1);
        check(KeyQueries.<Integer> builder().descending().ge(5).build(), 9, 8, 7, 6);
        check(KeyQueries.<Integer> builder().descending().ge(6).build(), 9, 8, 7, 6);
        check(KeyQueries.<Integer> builder().descending().ge(9).build(), 9);
        check(KeyQueries.<Integer> builder().descending().ge(10).build());
    }

    @Test
    public void testEqualsTo() {
        check(KeyQueries.eq(4), 4);
        check(KeyQueries.eq(1), 1);
        check(KeyQueries.eq(9), 9);
        check(KeyQueries.eq(-1));
        check(KeyQueries.eq(5));
        check(KeyQueries.eq(10));
    }

    @Test
    public void testGreater() {
        check(KeyQueries.gt(-1), 1, 2, 3, 4, 6, 7, 8, 9);
        check(KeyQueries.gt(1), 2, 3, 4, 6, 7, 8, 9);
        check(KeyQueries.gt(5), 6, 7, 8, 9);
        check(KeyQueries.gt(6), 7, 8, 9);
        check(KeyQueries.gt(9));
        check(KeyQueries.gt(10));
    }

    @Test
    public void testFirstLast() {
        check(KeyQueries.first(), 1);
        check(KeyQueries.last(), 9);
    }

    @Test
    public void testNext() {
        check(KeyQueries.next(-1), 1);
        check(KeyQueries.next(1), 2);
        check(KeyQueries.next(4), 6);
        check(KeyQueries.next(9));
    }

    @Test
    public void testLess() {
        check(KeyQueries.lt(-1));
        check(KeyQueries.lt(1));
        check(KeyQueries.lt(2), 1);
        check(KeyQueries.lt(5), 1, 2, 3, 4);
        check(KeyQueries.lt(6), 1, 2, 3, 4);
        check(KeyQueries.lt(9), 1, 2, 3, 4, 6, 7, 8);
        check(KeyQueries.lt(10), 1, 2, 3, 4, 6, 7, 8, 9);
    }

    @Test
    public void testLessOrEq() {
        check(KeyQueries.le(-1));
        check(KeyQueries.le(1), 1);
        check(KeyQueries.le(2), 1, 2);
        check(KeyQueries.le(5), 1, 2, 3, 4);
        check(KeyQueries.le(6), 1, 2, 3, 4, 6);
        check(KeyQueries.le(9), 1, 2, 3, 4, 6, 7, 8, 9);
        check(KeyQueries.le(10), 1, 2, 3, 4, 6, 7, 8, 9);
    }

    @Test
    public void testRange() {
        check(KeyQueries.range(-1, true, 10, true), 1, 2, 3, 4, 6, 7, 8, 9);
        check(KeyQueries.range(-1, false, 10, false), 1, 2, 3, 4, 6, 7, 8, 9);
        check(KeyQueries.range(0, true, 9, false), 1, 2, 3, 4, 6, 7, 8);
        check(KeyQueries.range(1, false, 9, true), 2, 3, 4, 6, 7, 8, 9);
        check(KeyQueries.range(2, true, 7, false), 2, 3, 4, 6);
        check(KeyQueries.range(2, true, 6, true), 2, 3, 4, 6);
        check(KeyQueries.range(5, true, 6, false));
        check(KeyQueries.range(6, true, 6, true), 6);
        check(KeyQueries.range(6, true, 6, false));
        check(KeyQueries.range(6, false, 6, true));
        check(KeyQueries.range(5, true, 2, true));
    }

    @Test
    public void testRangeDesc() {
        check(KeyQueries.<Integer> builder().descending().ge(2).lt(8).build(), 7, 6, 4, 3, 2);
        check(KeyQueries.<Integer> builder().descending().ge(8).lt(8).build());
        check(KeyQueries.<Integer> builder().descending().gt(0).lt(3).build(), 2, 1);
        check(KeyQueries.<Integer> builder().le(7).descending().gt(5).build(), 7, 6);
        check(KeyQueries.<Integer> builder().lt(10).descending().ge(5).build(), 9, 8, 7, 6);
        check(KeyQueries.<Integer> builder().le(10).descending().ge(5).build(), 9, 8, 7, 6);
        check(KeyQueries.<Integer> builder().lt(9).descending().ge(5).build(), 8, 7, 6);
        check(KeyQueries.<Integer> builder().le(9).descending().ge(5).build(), 9, 8, 7, 6);
    }

    @Test
    public void testLimitOffset() {
        check(KeyQueries.<Integer> builder().ge(2).lt(8).limit(2).build(), 2, 3);
        check(KeyQueries.<Integer> builder().ge(2).lt(8).limit(10).build(), 2, 3, 4, 6, 7);
        check(KeyQueries.<Integer> builder().ge(2).lt(8).limit(10).offset(2).build(), 4, 6, 7);
        check(KeyQueries.<Integer> builder().ge(2).lt(8).limit(2).offset(2).build(), 4, 6);
        check(KeyQueries.<Integer> builder().ge(2).lt(8).limit(1).offset(10).build());
        check(KeyQueries.<Integer> builder().ge(2).lt(8).limit(1).descending().offset(1).build(), 6);
        check(KeyQueries.<Integer> builder().ge(2).lt(8).limit(10).descending().offset(1).build(), 6, 4, 3, 2);
        check(KeyQueries.<Integer> builder().ge(2).lt(8).limit(1).descending().offset(4).build(), 2);
        check(KeyQueries.<Integer> builder().limit(4).build(), 1, 2, 3, 4);
        check(KeyQueries.<Integer> builder().limit(4).descending().build(), 9, 8, 7, 6);
        check(KeyQueries.<Integer> builder().limit(4).offset(2).build(), 3, 4, 6, 7);
        check(KeyQueries.<Integer> builder().limit(4).descending().offset(5).build(), 3, 2, 1);
    }

    @Test
    public void testDelete() {
        check(KeyQueries.<Integer> builder().build(), 1, 2, 3, 4, 6, 7, 8, 9);
        MULTI.disconnectKeys(velvet, root.getKey(), 3);
        check(KeyQueries.<Integer> builder().build(), 1, 2, 4, 6, 7, 8, 9);
        MULTI.disconnectKeys(velvet, root.getKey(), 1);
        check(KeyQueries.<Integer> builder().build(), 2, 4, 6, 7, 8, 9);
        MULTI.disconnectKeys(velvet, root.getKey(), 9);
        check(KeyQueries.<Integer> builder().build(), 2, 4, 6, 7, 8);
        MULTI.disconnectKeys(velvet, root.getKey(), 4);
        check(KeyQueries.<Integer> builder().build(), 2, 6, 7, 8);
        MULTI.disconnectKeys(velvet, root.getKey(), 1); // NOOP
        check(KeyQueries.<Integer> builder().build(), 2, 6, 7, 8);
        MULTI.disconnectKeys(velvet, root.getKey(), 7);
        check(KeyQueries.<Integer> builder().build(), 2, 6, 8);
        MULTI.disconnectKeys(velvet, root.getKey(), 2);
        check(KeyQueries.<Integer> builder().build(), 6, 8);
        MULTI.disconnectKeys(velvet, root.getKey(), 8);
        check(KeyQueries.<Integer> builder().build(), 6);
        MULTI.disconnectKeys(velvet, root.getKey(), 6);
        check(KeyQueries.<Integer> builder().build());
    }

    @Test
    public void testAllKeys() {
        int[] keys = MULTI.get(velvet, root).stream().mapToInt(TestEnt2::getKey).toArray();
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

    private void check(ISingleReturnKeyQuery<Integer> query, int... v) {
        int[] keys = MULTI.indexed((IKeyQuery<Integer>)query).get(velvet, root).stream().mapToInt(TestEnt2::getKey).toArray();
        Assert.assertArrayEquals(v, keys);
    }

}
