package com.zakgof.db.velvet.structure.stack;

import java.util.ArrayList;
import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetUtil;
import com.zakgof.db.velvet.links.SingleLinkDef;
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
  
  public IStack<T> getStack(IVelvet velvet, N node) {
    return new StackImpl(velvet, node);
  }
  
  private class StackImpl implements IStack<T> {
    
    private static final long LOCK_TIMEOUT = 30000;
    private final IVelvet velvet;
    private final N node;

    public StackImpl(IVelvet velvet, N node) {
      this.velvet = velvet;
      this.node = node;
    }

    private <R> R locked(IProvider<R> IProvider) {
      String lockKey = "stack-" + VelvetUtil.keyOf(node);
      try {
        velvet.raw().lock(lockKey, LOCK_TIMEOUT); // TODO : velvet config
        return IProvider.get();
      } finally {
        velvet.raw().unlock(lockKey); // TODO : AOP
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
      velvet.put(element);
      StackElement<T> oldHead = rootLink.single(velvet, node);
      if (oldHead != null) {
        rootLink.disconnect(velvet, node, oldHead);
        link.connect(velvet, element, oldHead);
      }
      rootLink.connect(velvet, node, element);
    }
    
    private T lockedPop() {
      StackElement<T> head = rootLink.single(velvet, node);
      if (head != null) {
        StackElement<T> next = link.single(velvet, head);
        rootLink.disconnect(velvet, node, head);
        if (next != null)
          rootLink.connect(velvet, node, next);
        velvet.delete(head);
        return head.get();
      }
      return null;
    }
    
    private T lockedPeek() {
      StackElement<T> head = rootLink.single(velvet, node);
      if (head != null)
        return head.get();
      return null;
    }
    
    private List<T> lockedTop(int count) {
      List<T> list = new ArrayList<T>(count);
      StackElement<T> element = rootLink.single(velvet, node);
      for (int i=0; i<count && element != null; i++) {
        list.add(element.get());
        element = link.single(velvet, element);
      }
      return list;
      
    }
    
  }

}
