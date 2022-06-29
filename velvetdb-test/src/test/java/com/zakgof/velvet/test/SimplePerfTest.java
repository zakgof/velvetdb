package com.zakgof.velvet.test;

import com.zakgof.velvet.entity.Entities;
import com.zakgof.velvet.entity.IEntityDef;
import com.zakgof.velvet.test.defs.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimplePerfTest extends AVelvetTest {

    private final IEntityDef<String, Person> personEntity = Entities.create(Person.class);
    private List<Person> batch;
    private List<Person> shuffledBatch;

    @BeforeEach
    void setup() {
        batch = IntStream.range(0, 1_000_000)
                .mapToObj(i -> new Person("AX" + i, "John", "Smith" + i))
                .collect(Collectors.toList());

        shuffledBatch = new ArrayList<>(batch);
        Collections.shuffle(batch);
    }

    @Test
    void batchSeqPut() {
        personEntity.put()
                .values(batch)
                .execute(velvetEnv);
    }

    @Test
    void batchRndPut() {

        personEntity.put()
                .values(batch)
                .execute(velvetEnv);
    }
}
