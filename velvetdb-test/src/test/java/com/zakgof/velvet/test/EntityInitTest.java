package com.zakgof.velvet.test;

import com.zakgof.velvet.VelvetException;
import com.zakgof.velvet.entity.Entities;
import com.zakgof.velvet.entity.IEntityDef;
import com.zakgof.velvet.test.defs.NoKey;
import com.zakgof.velvet.test.defs.OneKey;
import com.zakgof.velvet.test.defs.TwoKeys;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityInitTest {

    @Test
    void throwErrorOnNoKey() {
        assertThatThrownBy(() -> Entities.create(NoKey.class))
                .isInstanceOf(VelvetException.class);
    }

    @Test
    void throwErrorOnTwoKeys() {
        assertThatThrownBy(() -> Entities.create(TwoKeys.class))
                .isInstanceOf(VelvetException.class);
    }

    @Test
    void acceptOneKey() {
        IEntityDef<String, OneKey> entity = Entities.create(OneKey.class);

        assertThat(entity.valueClass()).isEqualTo(OneKey.class);
        assertThat(entity.keyClass()).isEqualTo(String.class);
        assertThat(entity.kind()).isEqualTo("onekey");
        assertThat(entity.keyOf(new OneKey("key"))).isEqualTo("key");
    }

}
