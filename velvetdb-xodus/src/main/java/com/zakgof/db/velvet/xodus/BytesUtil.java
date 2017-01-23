package com.zakgof.db.velvet.xodus;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Date;

import com.google.common.primitives.Primitives;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.ByteBinding;
import jetbrains.exodus.bindings.DoubleBinding;
import jetbrains.exodus.bindings.FloatBinding;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.bindings.ShortBinding;
import jetbrains.exodus.bindings.StringBinding;

public class BytesUtil {

    public static ByteIterable join(ByteIterable p1, ByteIterable p2) {
        int length;
        byte[] bytes;
        byte[] p1bytes = p1.getBytesUnsafe();
        if (p1.getLength() > 65535)
            throw new IllegalArgumentException();
        length = 2 + p1.getLength() + (p2 == null ? 0 : p2.getLength());
        bytes = new byte[length];
        bytes[0] = (byte) (p1.getLength() >> 8);
        bytes[1] = (byte) (p1.getLength() & 0xFF);
        System.arraycopy(p1bytes, 0, bytes, 2, p1.getLength());
        if (p2 != null) {
            byte[] p2bytes = p2.getBytesUnsafe();
            System.arraycopy(p2bytes, 0, bytes, 2 + p1.getLength(), p2.getLength());
        }
        return new ArrayByteIterable(bytes);
    }

    public static ByteIterable extract1(ByteIterable key) {
        byte[] bytes = key.getBytesUnsafe();
        int length = (bytes[0] << 8) | bytes[1];
        byte[] part = new byte[length];
        System.arraycopy(bytes, 2, part, 0, length);
        return new ArrayByteIterable(part);
    }

    public static ByteIterable extract2(ByteIterable key) {
        byte[] bytes = key.getBytesUnsafe();
        int length = (bytes[0] << 8) | bytes[1];
        byte[] part = new byte[key.getLength() - 2 - length];
        System.arraycopy(bytes, 2 + length, part, 0, key.getLength() - 2 - length);
        return new ArrayByteIterable(part);
    }

    public static ByteIterable keyToBi(Object obj) {
        if (obj instanceof Byte)
            return ByteBinding.byteToEntry((byte) (Byte) obj);
        else if (obj instanceof Short)
            return ShortBinding.shortToEntry((short) (Short) obj);
        else if (obj instanceof Integer)
            return IntegerBinding.intToEntry((int) (Integer) obj);
        else if (obj instanceof Long)
            return LongBinding.longToEntry((long) (Long) obj);
        else if (obj instanceof Float)
            return FloatBinding.floatToEntry((float) (Float) obj);
        else if (obj instanceof Double)
            return DoubleBinding.doubleToEntry((double) (Double) obj);
        else if (obj instanceof String)
            return StringBinding.stringToEntry((String) obj);
        else if (obj instanceof Date)
            return keyToBi(((Date) obj).getTime());
        else if (obj instanceof LocalDate)
            return keyToBi(((LocalDate) obj).getLong(ChronoField.EPOCH_DAY));
        else if (obj instanceof LocalDateTime)
            return keyToBi(((LocalDateTime) obj).toInstant(ZoneOffset.UTC).toEpochMilli());

        throw new UnsupportedOperationException("Unsupported key type: " + obj.getClass());
    }

    public static <K> K keyBiToObj(Class<K> cl, ByteIterable bi) {
        Class<K> clazz = Primitives.wrap(cl);
        if (clazz == Byte.class || clazz == byte.class) {
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
