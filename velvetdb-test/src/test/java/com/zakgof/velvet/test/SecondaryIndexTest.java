package com.zakgof.velvet.test;

import com.zakgof.velvet.entity.Entities;
import com.zakgof.velvet.request.IEntityDef;
import com.zakgof.velvet.test.defs.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class SecondaryIndexTest extends AVelvetTest {

    private final IEntityDef<String, Person> personEntity = Entities.from(Person.class)
            .index("ln", Person::lastName, String.class)
            .make();

    @BeforeEach
    void setup() {

        List<Person> batch = IntStream.range(0, 50)
                .mapToObj(i -> new Person(String.format("AX%02d", i), "John", "Smith" + (i % 10)))
                .collect(Collectors.toList());

        personEntity.put()
                .values(batch)
                .execute(velvetEnv);
    }

    @Test
    void eq() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .eq("Smith7")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX07", "AX17", "AX27", "AX37", "AX47"));
    }

    @Test
    void eqDesc() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .eq("Smith7")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX47", "AX37", "AX27", "AX17", "AX07"));
    }

    @Test
    void eqAbsentUp() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .eq("Smither")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void eqAbsentUpDesc() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .eq("Smither")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void eqAbsentDown() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .eq("Smi")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void eqAbsentDownDesc() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .eq("Smi")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void eqAbsentMiddle() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .eq("Smith85")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void eqAbsentMiddleDesc() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .eq("Smith85")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void gt() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gt("Smith8")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX09", "AX19", "AX29", "AX39", "AX49"));
    }

    @Test
    void gtMiddle() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gt("Smith8888")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX09", "AX19", "AX29", "AX39", "AX49"));
    }

    @Test
    void gtDesc() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gt("Smith8")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX49", "AX39", "AX29", "AX19", "AX09"));
    }

    @Test
    void gtDescMiddle() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gt("Smith8888")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX49", "AX39", "AX29", "AX19", "AX09"));
    }

    @Test
    void gte() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gte("Smith8")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX08", "AX18", "AX28", "AX38", "AX48", "AX09", "AX19", "AX29", "AX39", "AX49"));
    }

    @Test
    void gteDesc() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gte("Smith8")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX49", "AX39", "AX29", "AX19", "AX09", "AX48", "AX38", "AX28", "AX18", "AX08"));
    }

    @Test
    void gtLast() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gt("Smith9")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void gtLastDesc() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gt("Smith9")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void gteLast() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gte("Smith9")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX09", "AX19", "AX29", "AX39", "AX49"));
    }

    @Test
    void gteLastDesc() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gte("Smith9")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX49", "AX39", "AX29", "AX19", "AX09"));
    }

    @Test
    void gteNone() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gte("Smith99")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void gteNoneDesc() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gte("Smith99")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void lt() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .lt("Smith1")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX00", "AX10", "AX20", "AX30", "AX40"));
    }

    @Test
    void ltDesc() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .lt("Smith1")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX40", "AX30", "AX20", "AX10", "AX00"));
    }

    @Test
    void ltDescMiddle() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .lt("Smith0000")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX40", "AX30", "AX20", "AX10", "AX00"));
    }

    @Test
    void ltDescAll() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .lt("Smith99")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of(
                "AX49", "AX39", "AX29", "AX19", "AX09",
                "AX48", "AX38", "AX28", "AX18", "AX08",
                "AX47", "AX37", "AX27", "AX17", "AX07",
                "AX46", "AX36", "AX26", "AX16", "AX06",
                "AX45", "AX35", "AX25", "AX15", "AX05",
                "AX44", "AX34", "AX24", "AX14", "AX04",
                "AX43", "AX33", "AX23", "AX13", "AX03",
                "AX42", "AX32", "AX22", "AX12", "AX02",
                "AX41", "AX31", "AX21", "AX11", "AX01",
                "AX40", "AX30", "AX20", "AX10", "AX00"));
    }

    @Test
    void ltDescLast() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .lt("Smith9")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of(
                "AX48", "AX38", "AX28", "AX18", "AX08",
                "AX47", "AX37", "AX27", "AX17", "AX07",
                "AX46", "AX36", "AX26", "AX16", "AX06",
                "AX45", "AX35", "AX25", "AX15", "AX05",
                "AX44", "AX34", "AX24", "AX14", "AX04",
                "AX43", "AX33", "AX23", "AX13", "AX03",
                "AX42", "AX32", "AX22", "AX12", "AX02",
                "AX41", "AX31", "AX21", "AX11", "AX01",
                "AX40", "AX30", "AX20", "AX10", "AX00"));
    }

    @Test
    void lte() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .lte("Smith1")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX00", "AX10", "AX20", "AX30", "AX40", "AX01", "AX11", "AX21", "AX31", "AX41"));
    }

    @Test
    void lteDesc() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .lte("Smith1")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX41", "AX31", "AX21", "AX11", "AX01", "AX40", "AX30", "AX20", "AX10", "AX00"));
    }

    @Test
    void lteDescMiddle() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .lte("Smith1111")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of("AX41", "AX31", "AX21", "AX11", "AX01", "AX40", "AX30", "AX20", "AX10", "AX00"));
    }

    @Test
    void lteNone() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .lt("Smith")
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void lteFirstDesc() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .lt("Smith0")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEmpty();
    }

    @Test
    void lteLastDesc() {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .lte("Smith9")
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertThat(keys).isEqualTo(List.of(
                "AX49", "AX39", "AX29", "AX19", "AX09",
                "AX48", "AX38", "AX28", "AX18", "AX08",
                "AX47", "AX37", "AX27", "AX17", "AX07",
                "AX46", "AX36", "AX26", "AX16", "AX06",
                "AX45", "AX35", "AX25", "AX15", "AX05",
                "AX44", "AX34", "AX24", "AX14", "AX04",
                "AX43", "AX33", "AX23", "AX13", "AX03",
                "AX42", "AX32", "AX22", "AX12", "AX02",
                "AX41", "AX31", "AX21", "AX11", "AX01",
                "AX40", "AX30", "AX20", "AX10", "AX00"));
    }
}
