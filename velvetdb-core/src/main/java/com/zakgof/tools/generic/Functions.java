package com.zakgof.tools.generic;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.OptionalDouble;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Functions {

  public static <T> IFunction<T, T> identity() {
    return new IFunction<T, T>() {
      @Override
      public T get(T arg) {
        return arg;
      }
    };
  }

  public static <T extends Comparable<T>> Comparator<T> comparator() {
    return new Comparator<T>() {
      @Override
      public int compare(T o1, T o2) {
        return o1.compareTo(o2);
      }
    };
  }

  public static <T extends Comparable<T>> Comparator<T> reverseComparator() {
    return new Comparator<T>() {
      @Override
      public int compare(T o1, T o2) {
        return -o1.compareTo(o2);
      }
    };
  }

  public static <T, K extends Comparable<K>> Comparator<T> comparator(IFunction<T, K> getter) {
    return new Comparator<T>() {
      @Override
      public int compare(T o1, T o2) {
        return getter.get(o1).compareTo(getter.get(o2));
      }
    };
  }

  public static <T, K extends Comparable<K>> Comparator<T> reverseComparator(IFunction<T, K> getter) {
    return new Comparator<T>() {
      @Override
      public int compare(T o1, T o2) {
        return -getter.get(o1).compareTo(getter.get(o2));
      }
    };
  }

  public static <T> Supplier<T> firstNext(Supplier<T> first, Function<T, T> next) {
    return new Supplier<T>() {

      private T current;

      @Override
      public T get() {
        if (current == null)
          return (current = first.get());
        return (current = next.apply(current));
      }
    };
  }

  public static <K> K[] toArray(Class<K> clazz, Collection<K> collection) {
    @SuppressWarnings("unchecked")
    K[] array = (K[]) Array.newInstance(clazz, collection.size());
    int i = 0;
    for (K element : collection)
      array[i++] = element;
    return array;
  }

  @SuppressWarnings("unchecked")
  public static <K> K[] newArray(Class<K> clazz, int len) {
    return (K[]) Array.newInstance(clazz, len);
  }

  @SafeVarargs
  public static <K> K[] newArray(Class<K> clazz, K... entries) {
    return Arrays.copyOf(entries, entries.length, getArrayClass(clazz));
  }

  @SuppressWarnings("unchecked")
  public static <T> Class<T[]> getArrayClass(Class<T> clazz) {
    try {
      return (Class<T[]>) Class.forName("[L" + clazz.getName() + ";");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static <K> boolean contains(K[] array, K value) {
    for (K e : array)
      if (e.equals(value))
        return true;
    return false;
  }

  public static <K> int indexOf(K[] array, K value) {
    for (int i = 0; i < array.length; i++) {
      K e = array[i];
      if (e.equals(value))
        return i;
    }
    return -1;
  }

  public static DoubleStream stream(OptionalDouble f) {
    return f.isPresent() ? DoubleStream.of(f.getAsDouble()): DoubleStream.empty();
  }

  public static <T> Stream<T> stream(Iterator<T> iterator) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
        false);
    
  }

}
