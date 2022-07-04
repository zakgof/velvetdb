package com.zakgof.velvet.test;

import com.zakgof.velvet.entity.Entities;
import com.zakgof.velvet.entity.ISortedEntityDef;
import com.zakgof.velvet.test.defs.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.zakgof.velvet.test.AssertTools.assertKeyList;

public class DeleteTest extends AVelvetTest {

    private final ISortedEntityDef<String, Person> personEntity = Entities.from(Person.class)
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
    }

    @ParameterizedTest
    @CsvSource({
            "AX00,  AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08|AX09",
            "AX03,  AX00|AX01|AX02|AX04|AX05|AX06|AX07|AX08|AX09",
            "AX09,  AX00|AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08",
            "AX,    AX00|AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08|AX09",
            "AX055, AX00|AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08|AX09",
            "AX099, AX00|AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08|AX09",
    })
    void delete(String deleted, String expected) {

        personEntity.delete()
                .key(deleted)
                .execute(velvetEnv);

        List<String> remaining = personEntity.get()
                .all()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, remaining);
    }
}
