package com.zakgof.velvet.xodus;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.*;

import java.time.Instant;
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

    @SuppressWarnings("unchecked")
    public static <K> K deserialize(Class<K> clazz, ByteIterable bi) {
        if (bi.getLength() == 0) {
            return null;
        }
        if (clazz == Byte.class) {
            return clazz.cast(ByteBinding.entryToByte(bi));
        } else if (clazz == Short.class) {
            return clazz.cast(ShortBinding.entryToShort(bi));
        } else if (clazz == Integer.class) {
            return clazz.cast(IntegerBinding.entryToInt(bi));
        } else if (clazz == Long.class) {
            return clazz.cast(LongBinding.entryToLong(bi));
        } else if (clazz == Float.class) {
            return clazz.cast(FloatBinding.entryToFloat(bi));
        } else if (clazz == Double.class) {
            return clazz.cast(DoubleBinding.entryToDouble(bi));
        } else if (clazz == byte.class) {
            return (K)(Byte)ByteBinding.entryToByte(bi);
        } else if (clazz == short.class) {
            return (K)(Short)ShortBinding.entryToShort(bi);
        } else if (clazz == int.class) {
            return (K)(Integer)IntegerBinding.entryToInt(bi);
        } else if (clazz == long.class) {
            return (K)(Long)LongBinding.entryToLong(bi);
        } else if (clazz == float.class) {
            return (K)(Float)FloatBinding.entryToFloat(bi);
        } else if (clazz == double.class) {
            return (K)(Double)DoubleBinding.entryToDouble(bi);
        } else if (clazz == String.class) {
            return clazz.cast(StringBinding.entryToString(bi));
        } else if (clazz == Date.class) {
            return clazz.cast(new Date(LongBinding.entryToLong(bi)));
        } else if (clazz == LocalDate.class) {
            return clazz.cast(LocalDate.ofEpochDay(LongBinding.entryToLong(bi)));
        } else if (clazz == LocalDateTime.class) {
            return clazz.cast(LocalDateTime.ofInstant(Instant.ofEpochMilli(LongBinding.entryToLong(bi)), ZoneOffset.UTC));
        }

        throw new UnsupportedOperationException("Unsupported key type: " + clazz);
    }

}
