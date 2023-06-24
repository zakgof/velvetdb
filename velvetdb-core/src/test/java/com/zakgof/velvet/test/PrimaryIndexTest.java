package com.zakgof.velvet.test;

import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.entity.Entities;
import com.zakgof.velvet.entity.ISortedEntityDef;
import com.zakgof.velvet.test.base.InjectedVelvetProvider;
import com.zakgof.velvet.test.base.ProviderExtension;
import com.zakgof.velvet.test.defs.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.zakgof.velvet.test.base.AssertTools.assertKeyList;
import static com.zakgof.velvet.test.base.AssertTools.assertKeyListEmpty;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ProviderExtension.class)
public class PrimaryIndexTest {

    private final ISortedEntityDef<String, Person> personEntity = Entities.from(Person.class)
            .makeSorted();
    private final ISortedEntityDef<String, Person> personEntityEmpty = Entities.from(Person.class)
            .kind("person-empty")
            .makeSorted();


    @InjectedVelvetProvider
    private IVelvetEnvironment velvetEnv;

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

    @Test
    void getAllOrder() {
        List<String> keys = personEntity.get()
                .all()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList("AX00|AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08|AX09", keys);
    }

    @ParameterizedTest
    @CsvSource({
            "AX00,AX00",
            "AX05,AX05",
            "AX09,AX09",
            "AX0, ",
            "AX050, ",
            "AX091, "
    })
    void eq(String input, String expected) {
        List<String> keys = personEntity.index()
                .query()
                .eq(input)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "AX00,AX00",
            "AX05,AX05",
            "AX09,AX09",
            "AX0, ",
            "AX050, ",
            "AX091, "
    })
    void eqDesc(String input, String expected) {
        List<String> keys = personEntity.index()
                .query()
                .eq(input)
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @Test
    void first() {
        Person person = personEntity.index()
                .query()
                .first()
                .execute(velvetEnv);

        assertThat(person.passportNo()).isEqualTo("AX00");
    }

    @Test
    void last() {
        Person person = personEntity.index()
                .query()
                .last()
                .execute(velvetEnv);

        assertThat(person.passportNo()).isEqualTo("AX09");
    }

    @ParameterizedTest
    @CsvSource({
            "AX00, AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08|AX09",
            "AX05, AX06|AX07|AX08|AX09",
            "AX09, ",
            "AX0,  AX00|AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08|AX09",
            "AX050,AX06|AX07|AX08|AX09",
            "AX091,"
    })
    void gt(String input, String expected) {
        List<String> keys = personEntity.index()
                .query()
                .gt(input)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "AX00, AX09|AX08|AX07|AX06|AX05|AX04|AX03|AX02|AX01",
            "AX05, AX09|AX08|AX07|AX06",
            "AX09, ",
            "AX0,  AX09|AX08|AX07|AX06|AX05|AX04|AX03|AX02|AX01|AX00",
            "AX050,AX09|AX08|AX07|AX06",
            "AX091,"
    })
    void gtDesc(String input, String expected) {
        List<String> keys = personEntity.index()
                .query()
                .gt(input)
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "AX00, AX00|AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08|AX09",
            "AX05, AX05|AX06|AX07|AX08|AX09",
            "AX09, AX09",
            "AX0,  AX00|AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08|AX09",
            "AX050,AX06|AX07|AX08|AX09",
            "AX091,"
    })
    void gte(String input, String expected) {
        List<String> keys = personEntity.index()
                .query()
                .gte(input)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "AX00, AX09|AX08|AX07|AX06|AX05|AX04|AX03|AX02|AX01|AX00",
            "AX05, AX09|AX08|AX07|AX06|AX05",
            "AX09, AX09",
            "AX0,  AX09|AX08|AX07|AX06|AX05|AX04|AX03|AX02|AX01|AX00",
            "AX050,AX09|AX08|AX07|AX06",
            "AX091,"
    })
    void gteDesc(String input, String expected) {
        List<String> keys = personEntity.index()
                .query()
                .gte(input)
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "AX00, ",
            "AX05, AX00|AX01|AX02|AX03|AX04",
            "AX09, AX00|AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08",
            "AX0,  ",
            "AX050,AX00|AX01|AX02|AX03|AX04|AX05",
            "AX091,AX00|AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08|AX09",
    })
    void lt(String input, String expected) {
        List<String> keys = personEntity.index()
                .query()
                .lt(input)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "AX00, ",
            "AX05, AX04|AX03|AX02|AX01|AX00",
            "AX09, AX08|AX07|AX06|AX05|AX04|AX03|AX02|AX01|AX00",
            "AX0,  ",
            "AX050,AX05|AX04|AX03|AX02|AX01|AX00",
            "AX091,AX09|AX08|AX07|AX06|AX05|AX04|AX03|AX02|AX01|AX00",
    })
    void ltDesc(String input, String expected) {
        List<String> keys = personEntity.index()
                .query()
                .lt(input)
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "AX00, AX00",
            "AX05, AX00|AX01|AX02|AX03|AX04|AX05",
            "AX09, AX00|AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08|AX09",
            "AX0,  ",
            "AX050,AX00|AX01|AX02|AX03|AX04|AX05",
            "AX091,AX00|AX01|AX02|AX03|AX04|AX05|AX06|AX07|AX08|AX09",
    })
    void lte(String input, String expected) {
        List<String> keys = personEntity.index()
                .query()
                .lte(input)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "AX00, AX00",
            "AX05, AX05|AX04|AX03|AX02|AX01|AX00",
            "AX09, AX09|AX08|AX07|AX06|AX05|AX04|AX03|AX02|AX01|AX00",
            "AX0,  ",
            "AX050,AX05|AX04|AX03|AX02|AX01|AX00",
            "AX091,AX09|AX08|AX07|AX06|AX05|AX04|AX03|AX02|AX01|AX00",
    })
    void lteDesc(String input, String expected) {
        List<String> keys = personEntity.index()
                .query()
                .lte(input)
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @Test
    void firstEmpty() {
        Person person = personEntityEmpty.index()
                .query()
                .first()
                .execute(velvetEnv);

        assertThat(person).isNull();
    }

    @Test
    void lastEmpty() {
        Person person = personEntityEmpty.index()
                .query()
                .last()
                .execute(velvetEnv);

        assertThat(person).isNull();
    }

    @Test
    void ltEmpty() {
        List<String> keys = personEntityEmpty.index()
                .query()
                .lt("value")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyListEmpty(keys);
    }

    @Test
    void gtEmpty() {
        List<String> keys = personEntityEmpty.index()
                .query()
                .gt("value")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyListEmpty(keys);
    }

    @Test
    void lteEmpty() {
        List<String> keys = personEntityEmpty.index()
                .query()
                .lte("value")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyListEmpty(keys);
    }

    @Test
    void gteEmpty() {
        List<String> keys = personEntityEmpty.index()
                .query()
                .gte("value")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyListEmpty(keys);
    }



}
