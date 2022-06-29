package com.zakgof.velvet.test;

import com.zakgof.velvet.entity.Entities;
import com.zakgof.velvet.request.IEntityDef;
import com.zakgof.velvet.test.defs.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class GetTest extends AVelvetTest {

    private List<Person> batch;
    private final IEntityDef<String, Person> personEntity = Entities.create(Person.class);

    @BeforeEach
    void setup() {

        batch = IntStream.range(0, 100_000)
                .mapToObj(i -> new Person("AX" + i, "John", "Smith" + i))
                .collect(Collectors.toList());

        personEntity.put()
                .values(batch)
                .execute(velvetEnv);
    }

    @Test
    void get() {
        Person person = personEntity.get()
                .key("AX98765")
                .execute(velvetEnv);

        assertThat(person.passportNo()).isEqualTo("AX98765");
        assertThat(person.firstName()).isEqualTo("John");
        assertThat(person.lastName()).isEqualTo("Smith98765");
    }

    @Test
    void getAbsent() {
        Person person = personEntity.get()
                .key("BX1234")
                .execute(velvetEnv);

        assertThat(person).isNull();
    }

    @Test
    void getAll() {
        List<Person> persons = personEntity.get()
                .all()
                .asValueList()
                .execute(velvetEnv);

        assertThat(new HashSet<>(persons)).isEqualTo(new HashSet<>(batch));
    }

    @Test
    void getMulti() {

        Collection<String> keys = List.of("AX123", "AX9876", "AX8888");

        Map<String, Person> persons = personEntity.get()
                .keys(keys)
                .asMap()
                .execute(velvetEnv);

        assertThat(persons).isEqualTo(Map.of(
                "AX123", new Person("AX123", "John", "Smith123"),
                "AX9876", new Person("AX9876", "John", "Smith9876"),
                "AX8888", new Person("AX8888", "John", "Smith8888")
        ));
    }

    @Test
    void getSparseMulti() {

        Collection<String> keys = List.of("AX123", "BX9876", "AX8888");

        Map<String, Person> persons = personEntity.get()
                .keys(keys)
                .asMap()
                .execute(velvetEnv);

        assertThat(persons).isEqualTo(Map.of(
                "AX123", new Person("AX123", "John", "Smith123"),
                "AX8888", new Person("AX8888", "John", "Smith8888")
        ));
    }
}
