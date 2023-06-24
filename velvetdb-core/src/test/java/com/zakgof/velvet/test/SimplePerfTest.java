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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(ProviderExtension.class)
public class SimplePerfTest {

    private final static int RECORDS = 400;
    private final static int THREADS = 10;

    private final IEntityDef<String, Person> personEntity = Entities.create(Person.class);
    private List<Person> batch;

    @InjectedVelvetProvider
    private IVelvetEnvironment velvetEnv;

    @BeforeEach
    void setup() {
        batch = IntStream.range(0, RECORDS)
                .mapToObj(i -> new Person("AX" + i, "John", "Smith" + i))
                .collect(Collectors.toList());
    }

    @Test
    void concurrentPut() {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<? extends Future<?>> futures = IntStream.range(0, THREADS)
                .mapToObj(t -> pool.submit(() -> {
                    for (int c = 0; c < RECORDS; c++) {
                        personEntity.put()
                                .value(batch.get(c))
                                .execute(velvetEnv);
                    }
                })).collect(Collectors.toList());

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                fail(e);
            }
        });
    }

}