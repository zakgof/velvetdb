package com.zakgof.db.velvet.test;

import org.junit.Rule;

import com.zakgof.db.velvet.IVelvet;

public abstract class AVelvetTxnTest {

    @Rule
    public VelvetTxnRule rule = new VelvetTxnRule(this);

    public IVelvet velvet;

}
