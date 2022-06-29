package com.zakgof.velvet.test;

import com.zakgof.velvet.entity.Entities;
import com.zakgof.velvet.entity.ISortableEntityDef;
import com.zakgof.velvet.test.defs.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class PrimaryIndexTest extends AVelvetTest {

    private final ISortableEntityDef<String, Person> personEntity = Entities.from(Person.class)
            .makeSorted();
    private final ISortableEntityDef<String, Person> personEntityEmpty = Entities.from(Person.class)
            .kind("person-empty")
            .makeSorted();

    @BeforeEach
    void setup() {
        List<Person> batch = IntStream.range(0, 10)
                .mapToObj(i -> new Person(String.format("AX%02d", i), "John" + i, "Smith" + i))
                .collect(Collectors.toList());
        Collections.shuffle(batch);

        personEntity.put()
                .values(batch)
                .execute(velvetEnv);

        personEntityEmpty.initialize()
                .execute(velvetEnv);

    }

    @Test
    void testGetAllOrder() {
        List<String> keys = personEntity.get()
                .all()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX00", "AX01", "AX02", "AX03", "AX04", "AX05", "AX06", "AX07", "AX08", "AX09"));
    }

    @Test
    void testEq() {
        List<String> keys = personEntity.index()
                .query()
                .eq("AX05")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX05"));
    }

    @Test
    void testEqUp() {
        List<String> keys = personEntity.index()
                .query()
                .eq("AX109")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void testEqDown() {
        List<String> keys = personEntity.index()
                .query()
                .eq("AX")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void testEqMiddle() {
        List<String> keys = personEntity.index()
                .query()
                .eq("AX056")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void testFirst() {
        List<String> keys = personEntity.index()
                .query()
                .first()
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX00"));
    }

    @Test
    void testLast() {
        List<String> keys = personEntity.index()
                .query()
                .last()
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX09"));
    }

    @Test
    void testEqEmpty() {
        List<String> keys = personEntityEmpty.index()
                .query()
                .eq("AX05")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void testFirstEmpty() {
        List<String> keys = personEntityEmpty.index()
                .query()
                .first()
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void testLastEmpty() {
        List<String> keys = personEntityEmpty.index()
                .query()
                .last()
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }


}
