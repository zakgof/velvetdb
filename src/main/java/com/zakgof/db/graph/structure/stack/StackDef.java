package com.zakgof.db.graph.structure.stack;

import java.util.ArrayList;
import java.util.List;

import com.zakgof.db.graph.IPersister;
import com.zakgof.db.graph.PersisterUtil;
import com.zakgof.db.graph.datadef.SingleLinkDef;
import com.zakgof.tools.generic.IProvider;

class StackElement<T> {

  private T value;
  
  @SuppressWarnings("unused")
  @Deprecated
  private StackElement() {
  }

  StackElement(T value) {
    this.value = value;
  }
  
  T get() {
    return value;
  }
  
}

public class StackDef<N, T> {
  
  private final SingleLinkDef<N, StackElement<T>> rootLink;
  private final SingleLinkDef<StackElement<T>, StackElement<T>> link;
  
  public StackDef(Class<N> nClass, Class<T> elementClass, String edgeKind) {
    @SuppressWarnings("unchecked")
    Class<StackElement<T>> elType = (Class<StackElement<T>>)new StackElement<T>(null).getClass();
    rootLink = SingleLinkDef.of(nClass, elType, edgeKind);
    link = SingleLinkDef.of(elType, elType, "next");
  }
  
  public IStack<T> getStack(IPersister persister, N node) {
    return new StackImpl(persister, node);
  }
  
  private class StackImpl implements IStack<T> {
    
    private static final long LOCK_TIMEOUT = 30000;
    private final IPersister persister;
    private final N node;

    public StackImpl(IPersister persister, N node) {
      this.persister = persister;
      this.node = node;
    }

    private <R> R locked(IProvider<R> IProvider) {
      String lockKey = "stack-" + PersisterUtil.keyOf(node);
      try {
        persister.session().lock(lockKey, LOCK_TIMEOUT); // TODO : persister config
        return IProvider.get();
      } finally {
        persister.session().unlock(lockKey); // TODO : AOP
      }
    }
    
    @Override
    public T pop() {
      return locked(new IProvider<T>() {
        @Override
        public T get() {
          return lockedPop();
        }
      });
    }

    @Override
    public T peek() {
      return locked(new IProvider<T>() {
        @Override
        public T get() {
          return lockedPeek();
        }
      });
    }

    @Override
    public void push(final T value) {
      locked(new IProvider<Void>(){
        @Override
        public Void get() {
          lockedPush(value);
          return null;
        }
      });
    }

    @Override
    public List<T> top(final int count) {
      return locked(new IProvider<List<T>>() {
        @Override
        public List<T> get() {
          return lockedTop(count);
        }
      });
    }
    
    private void lockedPush(T value) {
      StackElement<T> element = new StackElement<T>(value);
      persister.put(element);
      StackElement<T> oldHead = rootLink.single(persister, node);
      if (oldHead != null) {
        rootLink.disconnect(persister, node, oldHead);
        link.connect(persister, element, oldHead);
      }
      rootLink.connect(persister, node, element);
    }
    
    private T lockedPop() {
      StackElement<T> head = rootLink.single(persister, node);
      if (head != null) {
        StackElement<T> next = link.single(persister, head);
        rootLink.disconnect(persister, node, head);
        if (next != null)
          rootLink.connect(persister, node, next);
        persister.delete(head);
        return head.get();
      }
      return null;
    }
    
    private T lockedPeek() {
      StackElement<T> head = rootLink.single(persister, node);
      if (head != null)
        return head.get();
      return null;
    }
    
    private List<T> lockedTop(int count) {
      List<T> list = new ArrayList<T>(count);
      StackElement<T> element = rootLink.single(persister, node);
      for (int i=0; i<count && element != null; i++) {
        list.add(element.get());
        element = link.single(persister, element);
      }
      return list;
      
    }
    
  }

}
