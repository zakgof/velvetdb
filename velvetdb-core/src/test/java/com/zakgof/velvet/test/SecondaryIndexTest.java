package com.zakgof.velvet.test;

import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.entity.Entities;
import com.zakgof.velvet.entity.IEntityDef;
import com.zakgof.velvet.test.base.InjectedVelvetProvider;
import com.zakgof.velvet.test.base.ProviderExtension;
import com.zakgof.velvet.test.defs.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.zakgof.velvet.test.base.AssertTools.assertKeyList;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ProviderExtension.class)
// TODO test offset
public class SecondaryIndexTest {

    private final IEntityDef<String, Person> personEntity = Entities.from(Person.class)
            .index("ln", Person::lastName, String.class)
            .make();

    @InjectedVelvetProvider
    private IVelvetEnvironment velvetEnv;

    @BeforeEach
    void setup() {

        List<Person> batch = IntStream.range(0, 50)
                .mapToObj(i -> new Person(String.format("AX%02d", i), "John", "Smith" + (i % 10)))
                .collect(Collectors.toList());

        personEntity.put()
                .values(batch)
                .execute(velvetEnv);
    }

    @ParameterizedTest
    @CsvSource({
            "Smith0, AX00|AX10|AX20|AX30|AX40",
            "Smith7, AX07|AX17|AX27|AX37|AX47",
            "Smith9, AX09|AX19|AX29|AX39|AX49",
            "Smith,",
            "Smith55,",
            "Smith99,"
    })
    void eq(String input, String expected) {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .eq(input)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "Smith0, AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49",
            "Smith7, AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49",
            "Smith9, ",
            "Smith,  AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49",
            "Smith55,AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49",
            "Smith99,"
    })
    void gt(String input, String expected) {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gt(input)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "Smith0, AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49",
            "Smith7, AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49",
            "Smith9, AX09|AX19|AX29|AX39|AX49",
            "Smith,  AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49",
            "Smith55,AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49",
            "Smith99,"
    })
    void gte(String input, String expected) {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gte(input)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "AX22, AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49",
            "AX47, AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49",
            "AX49, ",
            "AX00, AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49",
            "AX39, AX49"
    })
    void gtK(String key, String expected) {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gtK(key)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "Smith0, ",
            "Smith7, AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46",
            "Smith9, AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48",
            "Smith,  ",
            "Smith55,AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45",
            "Smith99,AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49"
    })
    void lt(String input, String expected) {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .lt(input)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "Smith0, AX00|AX10|AX20|AX30|AX40",
            "Smith7, AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47",
            "Smith9, AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49",
            "Smith,  ",
            "Smith55,AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45",
            "Smith99,AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49"
    })
    void lte(String input, String expected) {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .lte(input)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "AX00, ",
            "AX07, AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46",
            "AX49, AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39",
            "AX10 ,AX00",
            "AX43, AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33"
    })
    void ltK(String key, String expected) {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .ltK(key)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "Smith0, AX40|AX30|AX20|AX10|AX00",
            "Smith7, AX47|AX37|AX27|AX17|AX07",
            "Smith9, AX49|AX39|AX29|AX19|AX09",
            "Smith,",
            "Smith55,",
            "Smith99,"
    })
    void eqDesc(String input, String expected) {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .eq(input)
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "Smith0, AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08|AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01",
            "Smith7, AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08",
            "Smith9, ",
            "Smith,  AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08|AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00",
            "Smith55,AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08|AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06",
            "Smith99,"
    })
    void gtDesc(String input, String expected) {
        List<String> keys = personEntity.<String>index("ln")
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
            "Smith0, AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08|AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00",
            "Smith7, AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08|AX47|AX37|AX27|AX17|AX07",
            "Smith9, AX49|AX39|AX29|AX19|AX09",
            "Smith,  AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08|AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00",
            "Smith55,AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08|AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06",
            "Smith99,"
    })
    void gteDesc(String input, String expected) {
        List<String> keys = personEntity.<String>index("ln")
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
            "AX00, AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08|AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10",
            "AX47, AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08",
            "AX39, AX49",
            "AX48, AX49|AX39|AX29|AX19|AX09",
            "AX49,"
    })
    void gtKDesc(String key, String expected) {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .gtK(key)
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "Smith0, ",
            "Smith7, AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00",
            "Smith9, AX48|AX38|AX28|AX18|AX08|AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00",
            "Smith,  ",
            "Smith55,AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00",
            "Smith99,AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08|AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00"
    })
    void ltDesc(String input, String expected) {
        List<String> keys = personEntity.<String>index("ln")
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
            "Smith0, AX40|AX30|AX20|AX10|AX00",
            "Smith7, AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00",
            "Smith9, AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08|AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00",
            "Smith,  ",
            "Smith55,AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00",
            "Smith99,AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08|AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00"
    })
    void lteDesc(String input, String expected) {
        List<String> keys = personEntity.<String>index("ln").query()
                .lte(input)
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @ParameterizedTest
    @CsvSource({
            "AX00, ",
            "AX07, AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00",
            "AX08, AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00",
            "AX44, AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00"
    })
    void ltKDesc(String key, String expected) {
        List<String> keys = personEntity.<String>index("ln")
                .query()
                .ltK(key)
                .descending(true)
                .get()
                .asKeyList()
                .execute(velvetEnv);

        assertKeyList(expected, keys);
    }

    @Test
    void first() {
        Person person = personEntity.<String>index("ln")
                .query()
                .first()
                .execute(velvetEnv);

        assertThat(person.passportNo()).isEqualTo("AX00");
    }

    @Test
    void last() {
        Person person = personEntity.<String>index("ln")
                .query()
                .last()
                .execute(velvetEnv);

        assertThat(person.passportNo()).isEqualTo("AX49");
    }

    @Test
    void traverseForward() {
        List<String> keys = new ArrayList<>();
        for (Person person = personEntity.<String>index("ln")
                .query()
                .first()
                .execute(velvetEnv);
             person != null;
             person = personEntity.<String>index("ln")
                     .query()
                     .next(person)
                     .execute(velvetEnv)) {
            keys.add(person.passportNo());
        }
        assertKeyList("AX00|AX10|AX20|AX30|AX40|AX01|AX11|AX21|AX31|AX41|AX02|AX12|AX22|AX32|AX42|AX03|AX13|AX23|AX33|AX43|AX04|AX14|AX24|AX34|AX44|AX05|AX15|AX25|AX35|AX45|AX06|AX16|AX26|AX36|AX46|AX07|AX17|AX27|AX37|AX47|AX08|AX18|AX28|AX38|AX48|AX09|AX19|AX29|AX39|AX49", keys);
    }

    @Test
    void traverseBackward() {
        List<String> keys = new ArrayList<>();
        for (Person person = personEntity.<String>index("ln")
                .query()
                .last()
                .execute(velvetEnv);
             person != null;
             person = personEntity.<String>index("ln")
                     .query()
                     .prev(person)
                     .execute(velvetEnv)) {
            keys.add(person.passportNo());
        }
        assertKeyList("AX49|AX39|AX29|AX19|AX09|AX48|AX38|AX28|AX18|AX08|AX47|AX37|AX27|AX17|AX07|AX46|AX36|AX26|AX16|AX06|AX45|AX35|AX25|AX15|AX05|AX44|AX34|AX24|AX14|AX04|AX43|AX33|AX23|AX13|AX03|AX42|AX32|AX22|AX12|AX02|AX41|AX31|AX21|AX11|AX01|AX40|AX30|AX20|AX10|AX00", keys);
    }
}
