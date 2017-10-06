package com.zakgof.db.velvet.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.annimon.stream.Collectors;
import com.annimon.stream.Stream;
import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.ISecSortedMultiLinkDef;
import com.zakgof.db.velvet.link.Links;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.Queries;

public class SecondarySortedLinkTest extends AVelvetTxnTest {

  private IEntityDef<String, TestEnt> ENTITY = Entities.create(TestEnt.class);
  private IEntityDef<Integer, TestEnt3> ENTITY3 = Entities.from(TestEnt3.class).kind("realpojo").make(Integer.class, TestEnt3::getKey);
  private ISecSortedMultiLinkDef<String, TestEnt, Integer, TestEnt3, Long> MULTI = Links.sec(ENTITY, ENTITY3, Long.class, TestEnt3::getWeight);

  private TestEnt root;

  @Before
  public void init() {
    TestEnt3[] vals = new TestEnt3[] {
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
  private static final Object rSix = r("six-A",  "six-B");
  private static final Object rFour = r("four-A", "four-B", "four-C");


  @Test
  public void testGreaterOrEq() {
    check(Queries.greaterOrEq(-1L),     rOne, "two", "three", rFour, rSix);
    check(Queries.greaterOrEq(1L),      rOne, "two", "three", rFour, rSix);
    check(Queries.greaterOrEq(3L),      "three", rFour, rSix);
    check(Queries.greaterOrEq(4L),      rFour, rSix);
    check(Queries.greaterOrEq(5L),      rSix);
    check(Queries.greaterOrEq(6L),      rSix);
    check(Queries.greaterOrEq(7L)        );
  }

  @Test
  public void testIndexByKeyGreater() {
    check(Queries.<Integer, Long>builder().greaterKey(54).build(),     "two", "three", rFour, rSix);
    check(Queries.<Integer, Long>builder().greaterKey(44).build(),     "one-A", "two", "three", rFour, rSix);
    check(Queries.<Integer, Long>builder().greaterKey(33).build(),     "three", rFour, rSix);
    check(Queries.<Integer, Long>builder().greaterKey(21).build(),      rFour, rSix);
    check(Queries.<Integer, Long>builder().greaterKey(34).build(),      r("four-B", "four-C"), rSix);
    check(Queries.<Integer, Long>builder().greaterKey(47).build(),     "four-C", rSix);
    check(Queries.<Integer, Long>builder().greaterKey(60).build(),     rSix);
    check(Queries.<Integer, Long>builder().greaterKey(99).build()       );
    check(Queries.<Integer, Long>builder().greaterKey(31).build(),     "six-A");
  }

  @Test
  public void testIndexByKeyLess() {
    check(Queries.<Integer, Long>builder().lessKey(54).build(),     "one-B");
    check(Queries.<Integer, Long>builder().lessKey(44).build()      );
    check(Queries.<Integer, Long>builder().lessKey(33).build(),     rOne);
    check(Queries.<Integer, Long>builder().lessKey(21).build(),     rOne, "two");
    check(Queries.<Integer, Long>builder().lessKey(34).build(),     rOne, "two", "three");
    check(Queries.<Integer, Long>builder().lessKey(47).build(),     rOne, "two", "three", "four-A");
    check(Queries.<Integer, Long>builder().lessKey(60).build(),     rOne, "two", "three", r("four-A", "four-B"));
    check(Queries.<Integer, Long>builder().lessKey(99).build(),     rOne, "two", "three", rFour, "six-B");
    check(Queries.<Integer, Long>builder().lessKey(31).build(),     rOne, "two", "three", rFour);
  }

  @Test
  public void testGreaterOrEqDesc() {
    check(Queries.<Integer, Long>builder().descending().greaterOrEq(-1L).build(),     rSix, rFour, "three", "two", rOne);
    check(Queries.<Integer, Long>builder().descending().greaterOrEq(1L).build(),      rSix, rFour, "three", "two", rOne);
    check(Queries.<Integer, Long>builder().descending().greaterOrEq(3L).build(),      rSix, rFour, "three");
    check(Queries.<Integer, Long>builder().descending().greaterOrEq(4L).build(),      rSix, rFour);
    check(Queries.<Integer, Long>builder().descending().greaterOrEq(5L).build(),      rSix);
    check(Queries.<Integer, Long>builder().descending().greaterOrEq(6L).build(),      rSix);
    check(Queries.<Integer, Long>builder().descending().greaterOrEq(7L).build()      );
  }

  @Test
  public void testEqualsTo() {
    check(Queries.equalsTo(-1L)      );
    check(Queries.equalsTo(1L),      rOne);
    check(Queries.equalsTo(3L),      "three");
    check(Queries.equalsTo(4L),      rFour);
    check(Queries.equalsTo(5L)       );
    check(Queries.equalsTo(6L),      rSix);
    check(Queries.equalsTo(7L)       );
  }

  @Test
  public void testGreater() {
    check(Queries.greater(-1L),     rOne, "two", "three", rFour, rSix);
    check(Queries.greater(1L),      "two", "three", rFour, rSix);
    check(Queries.greater(3L),      rFour, rSix);
    check(Queries.greater(4L),      rSix);
    check(Queries.greater(5L),      rSix);
    check(Queries.greater(6L)        );
    check(Queries.greater(7L)        );
  }

  @Test
  public void testLess() {
    check(Queries.less(-1L)     );
    check(Queries.less(1L)      );
    check(Queries.less(3L),      rOne, "two");
    check(Queries.less(4L),      rOne, "two", "three");
    check(Queries.less(5L),      rOne, "two", "three", rFour);
    check(Queries.less(6L),      rOne, "two", "three", rFour);
    check(Queries.less(7L),      rOne, "two", "three", rFour, rSix );
  }

  @Test
  public void testLessDesc() {
    check(Queries.<Integer, Long>builder().less(-1L).descending().build());
    check(Queries.<Integer, Long>builder().less(1L).descending().build());
    check(Queries.<Integer, Long>builder().less(3L).descending().build(),	"two", rOne);
    check(Queries.<Integer, Long>builder().less(4L).descending().build(),   "three", "two", rOne);
    check(Queries.<Integer, Long>builder().less(5L).descending().build(),	rFour, "three", "two", rOne);
    check(Queries.<Integer, Long>builder().less(6L).descending().build(),	rFour, "three", "two", rOne);
    check(Queries.<Integer, Long>builder().less(7L).descending().build(), 	rSix, rFour, "three", "two", rOne);
  }

  @Test
  public void testLessOrEqDesc() {
    check(Queries.<Integer, Long>builder().lessOrEq(-1L).descending().build());
    check(Queries.<Integer, Long>builder().lessOrEq(1L).descending().build(), 	rOne);
    check(Queries.<Integer, Long>builder().lessOrEq(3L).descending().build(),	"three", "two", rOne);
    check(Queries.<Integer, Long>builder().lessOrEq(4L).descending().build(),   rFour, "three", "two", rOne);
    check(Queries.<Integer, Long>builder().lessOrEq(5L).descending().build(),	rFour, "three", "two", rOne);
    check(Queries.<Integer, Long>builder().lessOrEq(6L).descending().build(),	rSix, rFour, "three", "two", rOne);
    check(Queries.<Integer, Long>builder().lessOrEq(7L).descending().build(), 	rSix, rFour, "three", "two", rOne);
  }

  @Test
  public void testLessOrEq() {
    check(Queries.lessOrEq(-1L)     );
    check(Queries.lessOrEq(1L),      rOne);
    check(Queries.lessOrEq(3L),      rOne, "two", "three");
    check(Queries.lessOrEq(4L),      rOne, "two", "three", rFour);
    check(Queries.lessOrEq(5L),      rOne, "two", "three", rFour);
    check(Queries.lessOrEq(6L),      rOne, "two", "three", rFour, rSix);
    check(Queries.lessOrEq(7L),      rOne, "two", "three", rFour, rSix );
  }

  @Test
  public void testGreaterKey() {
    check(Queries. <Integer, Long>builder().greaterKey(21).build(),  rFour, rSix );
    check(Queries. <Integer, Long>builder().greaterKey(33).build(),  "three", rFour, rSix);
  }

  @Test
  public void testGreaterEqKey() {
    check(Queries. <Integer, Long>builder().greaterOrEqKey(21).build(), "three", rFour, rSix );
    check(Queries. <Integer, Long>builder().greaterOrEqKey(33).build(), "two", "three", rFour, rSix );
  }

  @Test
  public void testLessKey() {
    check(Queries. <Integer, Long>builder().lessKey(21).build(),  rOne, "two");
    check(Queries. <Integer, Long>builder().lessKey(33).build(),  rOne);
  }

  @Test
  public void testLessOrEqKey() {
    check(Queries. <Integer, Long>builder().lessOrEqKey(21).build(),  rOne, "two", "three");
    check(Queries. <Integer, Long>builder().lessOrEqKey(33).build(),  rOne, "two");
  }

  @Test
  public void testLessKeyDesc() {
    check(Queries. <Integer, Long>builder().lessKey(21).descending().build(),  "two", rOne);
    check(Queries. <Integer, Long>builder().lessKey(33).descending().build(),  rOne);
  }

  @Test
  public void testLessOrEqKeyDesc() {
    check(Queries. <Integer, Long>builder().lessOrEqKey(21).descending().build(),  "three", "two", rOne);
    check(Queries. <Integer, Long>builder().lessOrEqKey(33).descending().build(),  "two", rOne);
  }


  /*

  @Test
  public void testNext() {
    check(IndexQueryFactory.next(-1),        1);
    check(IndexQueryFactory.next(1),         2);
    check(IndexQueryFactory.next(4),         6);
    check(IndexQueryFactory.next(9));
  }

  @Test
  public void testRange() {
    check(IndexQueryFactory.range(-1, true, 10, true),      1, 2, 3, 4, 6, 7, 8, 9);
    check(IndexQueryFactory.range(-1, false, 10, false),    1, 2, 3, 4, 6, 7, 8, 9);
    check(IndexQueryFactory.range(0, true, 9, false),     1, 2, 3, 4, 6, 7, 8);
    check(IndexQueryFactory.range(1, false, 9, true),     2, 3, 4, 6, 7, 8, 9);
    check(IndexQueryFactory.range(2, true, 7, false),     2, 3, 4, 6);
    check(IndexQueryFactory.range(2, true, 6, true),      2, 3, 4, 6);
    check(IndexQueryFactory.range(5, true, 6, false));
    check(IndexQueryFactory.range(6, true, 6, true),      6);
    check(IndexQueryFactory.range(6, true, 6, false));
    check(IndexQueryFactory.range(6, false, 6, true));
    check(IndexQueryFactory.range(5, true, 2, true));
  }

  @Test
  public void testRangeDesc() {
    check(IndexQueryFactory.<Integer>builder().descending().greaterOrEq(2).less(8).build(),   7, 6, 4, 3, 2);
    check(IndexQueryFactory.<Integer>builder().descending().greaterOrEq(8).less(8).build());
    check(IndexQueryFactory.<Integer>builder().descending().greater(0).less(3).build(),   2, 1);
    check(IndexQueryFactory.<Integer>builder().lessOrEq(7).descending().greater(5).build(),  7, 6);
    check(IndexQueryFactory.<Integer>builder().less(10).descending().greaterOrEq(5).build(),  9, 8, 7, 6);
  }

  @Test
  public void testLimitOffset() {
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(2).build(),        2, 3);
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(10).build(),       2, 3, 4, 6, 7);
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(10).offset(2).build(),  4, 6, 7);
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(2).offset(2).build(),  4, 6);
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(1).offset(10).build());
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(1).descending().offset(1).build(),     6);
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(10).descending().offset(1).build(),     6, 4, 3, 2);
    check(IndexQueryFactory.<Integer>builder().greaterOrEq(2).less(8).limit(1).descending().offset(4).build(),      2);
    check(IndexQueryFactory.<Integer>builder().limit(4).build(),                     1, 2, 3, 4);
    check(IndexQueryFactory.<Integer>builder().limit(4).descending().build(),        9, 8, 7, 6);
    check(IndexQueryFactory.<Integer>builder().limit(4).offset(2).build(),                    3, 4, 6, 7);
    check(IndexQueryFactory.<Integer>builder().limit(4).descending().offset(5).build(),       3, 2, 1);
  }
  */

  @Test
  public void testDelete() {
    check(Queries.<Integer, Long>builder().build(),     rOne, "two", "three", rFour, rSix);
    MULTI.disconnectKeys(velvet, root.getKey(), 33);
    check(Queries.<Integer, Long>builder().build(),     rOne, "three", rFour, rSix);
    MULTI.disconnectKeys(velvet, root.getKey(), 31);
    check(Queries.<Integer, Long>builder().build(),     rOne, "three", rFour, "six-A");
    MULTI.disconnectKeys(velvet, root.getKey(), 54);
    check(Queries.<Integer, Long>builder().build(),     "one-B", "three", rFour, "six-A");
    MULTI.disconnectKeys(velvet, root.getKey(), 44);
    check(Queries.<Integer, Long>builder().build(),     "three", rFour, "six-A");
    MULTI.disconnectKeys(velvet, root.getKey(), 47);
    check(Queries.<Integer, Long>builder().build(),     "three", r("four-A", "four-C"), "six-A");
    MULTI.disconnectKeys(velvet, root.getKey(), 34);
    check(Queries.<Integer, Long>builder().build(),     "three", "four-C", "six-A");
    MULTI.disconnectKeys(velvet, root.getKey(), 21);
    check(Queries.<Integer, Long>builder().build(),     "four-C", "six-A");
    MULTI.disconnectKeys(velvet, root.getKey(), 99);
    check(Queries.<Integer, Long>builder().build(),     "four-C");
    MULTI.disconnectKeys(velvet, root.getKey(), 60);
    check(Queries.<Integer, Long>builder().build());
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
    List<String> result = Stream.of(MULTI.multi(velvet, root)).map(TestEnt3::getStr).collect(Collectors.toList());
    checkData(result, new Object[]{rOne, "two", "three", rFour, rSix});
  }

  private static Object r(String... s) {
    return s;
  }

  private void check(IRangeQuery<Integer, Long> query, Object...ref) {
    List<String> result = Stream.of(MULTI.indexed(query).multi(velvet, root)).map(TestEnt3::getStr).collect(Collectors.toList());
    checkData(result, ref);
  }

  private void checkData(List<String> result, Object... ref) {
    int i = 0;
    for (Object r : ref) {
      if (r instanceof String) {
        Assert.assertEquals("Mismatch at position " + i + ":", r, result.get(i));
        i++;
      } else if (r instanceof String[]) {
        String[] refarr = (String[])r;
        Set<String> refSet = new HashSet<>(Arrays.asList(refarr));
        Set<String> actSet = new HashSet<>(result.subList(i, i + refarr.length));
        Assert.assertEquals("Mismatch at position " + i + ":", refSet, actSet);
        i += refarr.length;
      }
    }
    Assert.assertEquals(i, result.size());
  }


}
