package com.zakgof.velvet.serializer;

import java.util.function.Supplier;

public interface ISerializerProvider extends Supplier<ISerializer> {
    String name();
}
