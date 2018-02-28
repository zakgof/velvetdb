package com.zakgof.db.velvet.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.ISecMultiLinkDef;
import com.zakgof.db.velvet.link.Links;
import com.zakgof.db.velvet.query.ISecQuery;
import com.zakgof.db.velvet.query.SecQueries;

// TODO: traverse using NEXT with non-unique metrics
public class SecondarySortedLinkTest extends AVelvetTxnTest {

    private IEntityDef<String, TestEnt> ENTITY = Entities.create(TestEnt.class);
    private IEntityDef<Integer, TestEnt3> ENTITY3 = Entities.from(TestEnt3.class).kind("sslt-realpojo").make(Integer.class, TestEnt3::getKey);
    private ISecMultiLinkDef<String, TestEnt, Integer, TestEnt3, Long> MULTI = Links.sec(ENTITY, ENTITY3, Long.class, TestEnt3::getWeight);

    private TestEnt root;

    @Before
    public void init() {
        vals = new TestEnt3[] {
               new TestEnt3(54, 1L, "one-A"),
               new TestEnt3(44, 1L, "one-B"),
               new TestEnt3(33, 2L, "two"),
               new TestEnt3(21, 3L, "three"),
               new TestEnt3(34, 4L, "four-A"),
               new TestEnt3(47, 4L, "four-B"),
               new TestEnt3(60, 4L, "four-C"),
               new TestEnt3(99, 6L, "six-A"),
               new TestEnt3(31, 6L, "six-B"),
        };
        root = new TestEnt("root", 1.0f);
        ENTITY.put(velvet, root);

        for (TestEnt3 val : vals) {
            ENTITY3.put(velvet, val);
            MULTI.connect(velvet, root, val);
        }
    }

    private static final Object rOne = r("one-A", "one-B");
    private static final Object rSix = r("six-A", "six-B");
    private static final Object rFour = r("four-A", "four-B", "four-C");
    private TestEnt3[] vals;

    @Test
    public void testge() {
        check(SecQueries.ge(-1L), rOne, "two", "three", rFour, rSix);
        check(SecQueries.ge(1L), rOne, "two", "three", rFour, rSix);
        check(SecQueries.ge(3L), "three", rFour, rSix);
        check(SecQueries.ge(4L), rFour, rSix);
        check(SecQueries.ge(5L), rSix);
        check(SecQueries.ge(6L), rSix);
        check(SecQueries.ge(7L));
    }

    @Test
    public void testIndexByKeyGreater() {
        check(SecQueries.<Integer, Long> builder().gtKey(54).build(), "two", "three", rFour, rSix);
        check(SecQueries.<Integer, Long> builder().gtKey(44).build(), "one-A", "two", "three", rFour, rSix); // TODO: ARGUABLE !
        check(SecQueries.<Integer, Long> builder().gtKey(33).build(), "three", rFour, rSix);
        check(SecQueries.<Integer, Long> builder().gtKey(21).build(), rFour, rSix);
        check(SecQueries.<Integer, Long> builder().gtKey(34).build(), r("four-B", "four-C"), rSix);
        check(SecQueries.<Integer, Long> builder().gtKey(47).build(), "four-C", rSix);
        check(SecQueries.<Integer, Long> builder().gtKey(60).build(), rSix);
        check(SecQueries.<Integer, Long> builder().gtKey(99).build());
        check(SecQueries.<Integer, Long> builder().gtKey(31).build(), "six-A");
    }

    @Test
    public void testIndexByKeyLess() {
        check(SecQueries.<Integer, Long> builder().ltKey(54).build(), "one-B"); // TODO: arguable
        check(SecQueries.<Integer, Long> builder().ltKey(44).build());
        check(SecQueries.<Integer, Long> builder().ltKey(33).build(), rOne);
        check(SecQueries.<Integer, Long> builder().ltKey(21).build(), rOne, "two");
        check(SecQueries.<Integer, Long> builder().ltKey(34).build(), rOne, "two", "three");
        check(SecQueries.<Integer, Long> builder().ltKey(47).build(), rOne, "two", "three", "four-A");
        check(SecQueries.<Integer, Long> builder().ltKey(60).build(), rOne, "two", "three", r("four-A", "four-B"));
        check(SecQueries.<Integer, Long> builder().ltKey(99).build(), rOne, "two", "three", rFour, "six-B");
        check(SecQueries.<Integer, Long> builder().ltKey(31).build(), rOne, "two", "three", rFour);
    }

    @Test
    public void testgeDesc() {
        check(SecQueries.<Integer, Long> builder().descending().ge(-1L).build(), rSix, rFour, "three", "two", rOne);
        check(SecQueries.<Integer, Long> builder().descending().ge(1L).build(), rSix, rFour, "three", "two", rOne);
        check(SecQueries.<Integer, Long> builder().descending().ge(3L).build(), rSix, rFour, "three");
        check(SecQueries.<Integer, Long> builder().descending().ge(4L).build(), rSix, rFour);
        check(SecQueries.<Integer, Long> builder().descending().ge(5L).build(), rSix);
        check(SecQueries.<Integer, Long> builder().descending().ge(6L).build(), rSix);
        check(SecQueries.<Integer, Long> builder().descending().ge(7L).build());
    }

    @Test
    public void testEqualsTo() {
        check(SecQueries.eq(-1L));
        check(SecQueries.eq(1L), rOne);
        check(SecQueries.eq(3L), "three");
        check(SecQueries.eq(4L), rFour);
        check(SecQueries.eq(5L));
        check(SecQueries.eq(6L), rSix);
        check(SecQueries.eq(7L));
    }

    @Test
    public void testGreater() {
        check(SecQueries.gt(-1L), rOne, "two", "three", rFour, rSix);
        check(SecQueries.gt(1L), "two", "three", rFour, rSix);
        check(SecQueries.gt(3L), rFour, rSix);
        check(SecQueries.gt(4L), rSix);
        check(SecQueries.gt(5L), rSix);
        check(SecQueries.gt(6L));
        check(SecQueries.gt(7L));
    }

    @Test
    public void testLess() {
        check(SecQueries.lt(-1L));
        check(SecQueries.lt(1L));
        check(SecQueries.lt(3L), rOne, "two");
        check(SecQueries.lt(4L), rOne, "two", "three");
        check(SecQueries.lt(5L), rOne, "two", "three", rFour);
        check(SecQueries.lt(6L), rOne, "two", "three", rFour);
        check(SecQueries.lt(7L), rOne, "two", "three", rFour, rSix);
    }

    @Test
    public void testLessDesc() {
        check(SecQueries.<Integer, Long> builder().lt(-1L).descending().build());
        check(SecQueries.<Integer, Long> builder().lt(1L).descending().build());
        check(SecQueries.<Integer, Long> builder().lt(3L).descending().build(), "two", rOne);
        check(SecQueries.<Integer, Long> builder().lt(4L).descending().build(), "three", "two", rOne);
        check(SecQueries.<Integer, Long> builder().lt(5L).descending().build(), rFour, "three", "two", rOne);
        check(SecQueries.<Integer, Long> builder().lt(6L).descending().build(), rFour, "three", "two", rOne);
        check(SecQueries.<Integer, Long> builder().lt(7L).descending().build(), rSix, rFour, "three", "two", rOne);
    }

    @Test
    public void testLessOrEqDesc() {
        check(SecQueries.<Integer, Long> builder().le(-1L).descending().build());
        check(SecQueries.<Integer, Long> builder().le(1L).descending().build(), rOne);
        check(SecQueries.<Integer, Long> builder().le(3L).descending().build(), "three", "two", rOne);
        check(SecQueries.<Integer, Long> builder().le(4L).descending().build(), rFour, "three", "two", rOne);
        check(SecQueries.<Integer, Long> builder().le(5L).descending().build(), rFour, "three", "two", rOne);
        check(SecQueries.<Integer, Long> builder().le(6L).descending().build(), rSix, rFour, "three", "two", rOne);
        check(SecQueries.<Integer, Long> builder().le(7L).descending().build(), rSix, rFour, "three", "two", rOne);
    }

    @Test
    public void testLessOrEq() {
        check(SecQueries.le(-1L));
        check(SecQueries.le(1L), rOne);
        check(SecQueries.le(3L), rOne, "two", "three");
        check(SecQueries.le(4L), rOne, "two", "three", rFour);
        check(SecQueries.le(5L), rOne, "two", "three", rFour);
        check(SecQueries.le(6L), rOne, "two", "three", rFour, rSix);
        check(SecQueries.le(7L), rOne, "two", "three", rFour, rSix);
    }

    @Test
    public void testGreaterKey() {
        check(SecQueries.<Integer, Long> builder().gtKey(21).build(), rFour, rSix);
        check(SecQueries.<Integer, Long> builder().gtKey(33).build(), "three", rFour, rSix);
    }

    @Test
    public void testGreaterEqKey() {
        check(SecQueries.<Integer, Long> builder().geKey(21).build(), "three", rFour, rSix);
        check(SecQueries.<Integer, Long> builder().geKey(33).build(), "two", "three", rFour, rSix);
    }

    @Test
    public void testLessKey() {
        check(SecQueries.<Integer, Long> builder().ltKey(21).build(), rOne, "two");
        check(SecQueries.<Integer, Long> builder().ltKey(33).build(), rOne);
    }

    @Test
    public void testLessOrEqKey() {
        check(SecQueries.<Integer, Long> builder().leKey(21).build(), rOne, "two", "three");
        check(SecQueries.<Integer, Long> builder().leKey(33).build(), rOne, "two");
    }

    @Test
    public void testLessKeyDesc() {
        check(SecQueries.<Integer, Long> builder().ltKey(21).descending().build(), "two", rOne);
        check(SecQueries.<Integer, Long> builder().ltKey(33).descending().build(), rOne);
    }

    @Test
    public void testLessOrEqKeyDesc() {
        check(SecQueries.<Integer, Long> builder().leKey(21).descending().build(), "three", "two", rOne);
        check(SecQueries.<Integer, Long> builder().leKey(33).descending().build(), "two", rOne);
    }

    /*
     *
     * @Test public void testNext() { check(IndexQueryFactory.next(-1), 1); check(IndexQueryFactory.next(1), 2); check(IndexQueryFactory.next(4), 6); check(IndexQueryFactory.next(9)); }
     *
     * @Test public void testRange() { check(IndexQueryFactory.range(-1, true, 10, true), 1, 2, 3, 4, 6, 7, 8, 9); check(IndexQueryFactory.range(-1, false, 10, false), 1, 2, 3, 4, 6, 7, 8, 9); check(IndexQueryFactory.range(0, true, 9, false), 1, 2,
     * 3, 4, 6, 7, 8); check(IndexQueryFactory.range(1, false, 9, true), 2, 3, 4, 6, 7, 8, 9); check(IndexQueryFactory.range(2, true, 7, false), 2, 3, 4, 6); check(IndexQueryFactory.range(2, true, 6, true), 2, 3, 4, 6);
     * check(IndexQueryFactory.range(5, true, 6, false)); check(IndexQueryFactory.range(6, true, 6, true), 6); check(IndexQueryFactory.range(6, true, 6, false)); check(IndexQueryFactory.range(6, false, 6, true)); check(IndexQueryFactory.range(5,
     * true, 2, true)); }
     *
     * @Test public void testRangeDesc() { check(IndexQueryFactory.<Integer>builder().descending().ge(2).lt(8).build(), 7, 6, 4, 3, 2); check(IndexQueryFactory.<Integer>builder().descending().ge(8).lt(8).build());
     * check(IndexQueryFactory.<Integer>builder().descending().gt(0).lt(3).build(), 2, 1); check(IndexQueryFactory.<Integer>builder().le(7).descending().gt(5).build(), 7, 6);
     * check(IndexQueryFactory.<Integer>builder().lt(10).descending().ge(5).build(), 9, 8, 7, 6); }
     *
     * @Test public void testLimitOffset() { check(IndexQueryFactory.<Integer>builder().ge(2).lt(8).limit(2).build(), 2, 3); check(IndexQueryFactory.<Integer>builder().ge(2).lt(8).limit(10).build(), 2, 3, 4, 6, 7);
     * check(IndexQueryFactory.<Integer>builder().ge(2).lt(8).limit(10).offset(2).build(), 4, 6, 7); check(IndexQueryFactory.<Integer>builder().ge(2).lt(8).limit(2).offset(2).build(), 4, 6);
     * check(IndexQueryFactory.<Integer>builder().ge(2).lt(8).limit(1).offset(10).build()); check(IndexQueryFactory.<Integer>builder().ge(2).lt(8).limit(1).descending().offset(1).build(), 6);
     * check(IndexQueryFactory.<Integer>builder().ge(2).lt(8).limit(10).descending().offset(1).build(), 6, 4, 3, 2); check(IndexQueryFactory.<Integer>builder().ge(2).lt(8).limit(1).descending().offset(4).build(), 2);
     * check(IndexQueryFactory.<Integer>builder().limit(4).build(), 1, 2, 3, 4); check(IndexQueryFactory.<Integer>builder().limit(4).descending().build(), 9, 8, 7, 6); check(IndexQueryFactory.<Integer>builder().limit(4).offset(2).build(), 3, 4, 6,
     * 7); check(IndexQueryFactory.<Integer>builder().limit(4).descending().offset(5).build(), 3, 2, 1); }
     */

    @Test
    public void testDelete() {
        check(SecQueries.<Integer, Long> builder().build(), rOne, "two", "three", rFour, rSix);
        MULTI.disconnectKeys(velvet, root.getKey(), 33);
        check(SecQueries.<Integer, Long> builder().build(), rOne, "three", rFour, rSix);
        MULTI.disconnectKeys(velvet, root.getKey(), 31);
        check(SecQueries.<Integer, Long> builder().build(), rOne, "three", rFour, "six-A");
        MULTI.disconnectKeys(velvet, root.getKey(), 54);
        check(SecQueries.<Integer, Long> builder().build(), "one-B", "three", rFour, "six-A");
        MULTI.disconnectKeys(velvet, root.getKey(), 44);
        check(SecQueries.<Integer, Long> builder().build(), "three", rFour, "six-A");
        MULTI.disconnectKeys(velvet, root.getKey(), 47);
        check(SecQueries.<Integer, Long> builder().build(), "three", r("four-A", "four-C"), "six-A");
        MULTI.disconnectKeys(velvet, root.getKey(), 34);
        check(SecQueries.<Integer, Long> builder().build(), "three", "four-C", "six-A");
        MULTI.disconnectKeys(velvet, root.getKey(), 21);
        check(SecQueries.<Integer, Long> builder().build(), "four-C", "six-A");
        MULTI.disconnectKeys(velvet, root.getKey(), 99);
        check(SecQueries.<Integer, Long> builder().build(), "four-C");
        MULTI.disconnectKeys(velvet, root.getKey(), 60);
        check(SecQueries.<Integer, Long> builder().build());
    }

    @Test
    public void testContains() {
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 33));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 31));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 54));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 44));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 47));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 34));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 21));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 99));
        Assert.assertTrue(MULTI.isConnectedKeys(velvet, root.getKey(), 60));

        Assert.assertFalse(MULTI.isConnectedKeys(velvet, root.getKey(), 39));
        Assert.assertFalse(MULTI.isConnectedKeys(velvet, root.getKey(), 38));
        Assert.assertFalse(MULTI.isConnectedKeys(velvet, root.getKey(), 20));
        Assert.assertFalse(MULTI.isConnectedKeys(velvet, root.getKey(), 100));
    }

    @Test
    public void testAllKeys() {
        List<String> result = MULTI.indexed(SecQueries.<Integer, Long>builder().build()).get(velvet, root).stream().map(TestEnt3::getStr).collect(Collectors.toList());
        checkData(result, new Object[] { rOne, "two", "three", rFour, rSix }); // TODO: arguable !
    }

    @Test
    public void testTraverseForward() {
        List<TestEnt3> entities = new ArrayList<>();
        TestEnt3 entity = MULTI.indexedSingle(SecQueries.first()).get(velvet, root);
        while (entity != null) {
            entities.add(entity);
            entity = MULTI.indexedSingle(SecQueries.<Integer, Long>next(entity.getKey())).get(velvet, root);
        }

        // Check if all present
        Assert.assertEquals(new HashSet<>(Arrays.asList(vals)), new HashSet<>(entities));
        TestEnt3 prevte = null;

        // Check order
        for (TestEnt3 te : entities) {
            if (prevte != null) {
                Assert.assertTrue(prevte.getWeight() <= te.getWeight());
                prevte = te;
            }
        }
    }

    @Test
    public void testTraverseBackward() {
        List<TestEnt3> entities = new ArrayList<>();
        TestEnt3 entity = MULTI.indexedSingle(SecQueries.last()).get(velvet, root);
        while (entity != null) {
            entities.add(entity);
            entity = MULTI.indexedSingle(SecQueries.<Integer, Long>prev(entity.getKey())).get(velvet, root);
        }

        // Check if all present
        Assert.assertEquals(new HashSet<>(Arrays.asList(vals)), new HashSet<>(entities));
        TestEnt3 prevte = null;

        // Check order
        for (TestEnt3 te : entities) {
            if (prevte != null) {
                Assert.assertTrue(prevte.getWeight() >= te.getWeight());
                prevte = te;
            }
        }
    }

    private static Object r(String... s) {
        return s;
    }

    private void check(ISecQuery<Integer, Long> query, Object... ref) {
        List<String> result = MULTI.indexed(query).get(velvet, root).stream().map(TestEnt3::getStr).collect(Collectors.toList());
        checkData(result, ref);
    }

    private void checkData(List<String> result, Object... ref) {
        int i = 0;
        for (Object r : ref) {
            if (result.size() <= i) {
                Assert.fail("Length mismatch, actual values: " + result);
            }
            if (r instanceof String) {
                Assert.assertEquals("Mismatch at position " + i + ":", r, result.get(i));
                i++;
            } else if (r instanceof String[]) {
                String[] refarr = (String[]) r;
                Set<String> refSet = new HashSet<>(Arrays.asList(refarr));
                Set<String> actSet = new HashSet<>(result.subList(i, i + refarr.length));
                Assert.assertEquals("Mismatch at position " + i + ":", refSet, actSet);
                i += refarr.length;
            }
        }
        Assert.assertEquals(i, result.size());
    }

}
