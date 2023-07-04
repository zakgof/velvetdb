package com.zakgof.velvet.serializer.migrator;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class EnumStructure {
    private final List<String> labels = new ArrayList<>();
}
