package com.zakgof.velvet.test.defs;

import com.zakgof.velvet.annotation.Key;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
public class Person {
    @Key
    private final String passportNo;
    private final String firstName;
    private final String lastName;
}
