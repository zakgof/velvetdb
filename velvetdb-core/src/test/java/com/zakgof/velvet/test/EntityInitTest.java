package com.zakgof.velvet.test;

import com.zakgof.velvet.VelvetException;
import com.zakgof.velvet.entity.Entities;
import com.zakgof.velvet.entity.IEntityDef;
import com.zakgof.velvet.test.defs.KeyMethodWithArgs;
import com.zakgof.velvet.test.defs.NoKey;
import com.zakgof.velvet.test.defs.OneFieldKey;
import com.zakgof.velvet.test.defs.OneMethodKey;
import com.zakgof.velvet.test.defs.TwoFieldKeys;
import com.zakgof.velvet.test.defs.TwoMethodKeys;
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
    void throwErrorOnTwoFieldKeys() {
        assertThatThrownBy(() -> Entities.create(TwoFieldKeys.class))
                .isInstanceOf(VelvetException.class);
    }

    @Test
    void throwErrorOnTwoMethodKeys() {
        assertThatThrownBy(() -> Entities.create(TwoMethodKeys.class))
                .isInstanceOf(VelvetException.class);
    }

    @Test
    void throwErrorOnMethodKeysWithArgs() {
        assertThatThrownBy(() -> Entities.create(KeyMethodWithArgs.class))
                .isInstanceOf(VelvetException.class);
    }

    @Test
    void acceptOneFieldKey() {
        IEntityDef<String, OneFieldKey> entity = Entities.create(OneFieldKey.class);

        assertThat(entity.valueClass()).isEqualTo(OneFieldKey.class);
        assertThat(entity.keyClass()).isEqualTo(String.class);
        assertThat(entity.kind()).isEqualTo("onefieldkey");
        assertThat(entity.keyOf(new OneFieldKey("key"))).isEqualTo("key");
    }

    @Test
    void acceptOneMethodKey() {
        IEntityDef<String, OneMethodKey> entity = Entities.create(OneMethodKey.class);

        assertThat(entity.valueClass()).isEqualTo(OneMethodKey.class);
        assertThat(entity.keyClass()).isEqualTo(String.class);
        assertThat(entity.kind()).isEqualTo("onemethodkey");
        assertThat(entity.keyOf(new OneMethodKey("keyval"))).isEqualTo("keyval");
    }
}
