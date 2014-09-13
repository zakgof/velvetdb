package com.zakgof.db.graph.structure.stack;

import java.util.List;

public interface IStack<T> {
  T pop();
  T peek();
  void push(T element);
  List<T> top(int count);
}