package com.zakgof.db.velvet.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IKeylessEntityDef;
import com.zakgof.db.velvet.query.Queries;

public class KeylessTest extends AVelvetTxnTest {

    private IKeylessEntityDef<KeylessEnt> ENTITY = Entities.keyless(KeylessEnt.class);

    @Before
    public void init() {
        // v1 v2 v3 v5 v7
        ENTITY.put(velvet, new KeylessEnt(7, "seven"));
        ENTITY.put(velvet, new KeylessEnt(5, "five"));
        ENTITY.put(velvet, new KeylessEnt(2, "two"));
        ENTITY.put(velvet, new KeylessEnt(3, "three"));
        ENTITY.put(velvet, new KeylessEnt(1, "one"));
    }

    @After
    public void cleanup() {
        List<Long> keys = ENTITY.keys(velvet);
        for (Long key : keys) {
            ENTITY.deleteKey(velvet, key);
        }
    }

    @Test
    public void testGetAll() {
        List<Integer> all = ENTITY.get(velvet).stream().map(KeylessEnt::getNum).collect(Collectors.toList());
        Assert.assertEquals(new HashSet<>(Arrays.asList(7, 5, 2, 3, 1)), new HashSet<>(all));
    }

    @Test
    public void testTraverse() {

        List<Integer> order = ENTITY.get(velvet).stream().map(KeylessEnt::getNum).collect(Collectors.toList());

        KeylessEnt first = ENTITY.get(velvet, Queries.first());
        Assert.assertEquals(order.get(0).intValue(), first.getNum());

        KeylessEnt e2 = ENTITY.get(velvet, Queries.nextKey(ENTITY.keyOf(first)));
        Assert.assertEquals(order.get(1).intValue(), e2.getNum());

        KeylessEnt e3 = ENTITY.get(velvet, Queries.nextKey(ENTITY.keyOf(e2))); // TODO: helper for this
        Assert.assertEquals(order.get(2).intValue(), e3.getNum());

        KeylessEnt last = ENTITY.get(velvet, Queries.last());
        Assert.assertEquals(order.get(4).intValue(), last.getNum());
    }

    @Test
    public void testGetByKey() {
        KeylessEnt first = ENTITY.get(velvet, Queries.first());
        Long k = ENTITY.keyOf(first);
        Assert.assertNotNull(k);
        KeylessEnt reget = ENTITY.get(velvet, k);
        Assert.assertEquals(first, reget);
    }

}
