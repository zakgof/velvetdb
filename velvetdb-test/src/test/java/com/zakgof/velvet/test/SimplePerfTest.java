package com.zakgof.velvet.test;

import com.zakgof.velvet.entity.Entities;
import com.zakgof.velvet.request.IEntityDef;
import com.zakgof.velvet.test.defs.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimplePerfTest extends AVelvetTest {

    private final IEntityDef<String, Person> personEntity = Entities.create(Person.class);
    private List<Person> batch;

    @BeforeEach
    void setup() {
        batch = IntStream.range(0, 1_000_000)
                .mapToObj(i -> new Person("AX" + i, "John", "Smith" + i))
                .collect(Collectors.toList());
    }

    @Test
    void batchSeqPut() {
        personEntity.put()
                .values(batch)
                .execute(velvetEnv);
    }

    @Test
    void batchRndPut() {

        Collections.shuffle(batch);

        personEntity.put()
                .values(batch)
                .execute(velvetEnv);
    }
}
