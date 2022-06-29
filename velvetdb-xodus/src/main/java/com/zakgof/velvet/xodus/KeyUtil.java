package com.zakgof.velvet.xodus;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Date;

public class KeyUtil {

    public static <T> ByteIterable serialize(T obj) {

        if (obj == null) {
            return new ArrayByteIterable(new byte[0]);
        }
        if (obj instanceof Byte)
            return ByteBinding.byteToEntry((Byte) obj);
        else if (obj instanceof Short)
            return ShortBinding.shortToEntry((Short) obj);
        else if (obj instanceof Integer)
            return IntegerBinding.intToEntry((Integer) obj);
        else if (obj instanceof Long)
            return LongBinding.longToEntry((Long) obj);
        else if (obj instanceof Float)
            return FloatBinding.floatToEntry((Float) obj);
        else if (obj instanceof Double)
            return DoubleBinding.doubleToEntry((Double) obj);
        else if (obj instanceof String)
            return StringBinding.stringToEntry((String) obj);
        else if (obj instanceof Date)
            return serialize(((Date) obj).getTime());
        else if (obj instanceof LocalDate)
            return serialize(((LocalDate) obj).getLong(ChronoField.EPOCH_DAY));
        else if (obj instanceof LocalDateTime)
            return serialize(((LocalDateTime) obj).toInstant(ZoneOffset.UTC).toEpochMilli());

        throw new UnsupportedOperationException("Unsupported key type: " + obj.getClass());

    }

}
