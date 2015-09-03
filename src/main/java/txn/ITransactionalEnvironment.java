package txn;

public interface ITransactionalEnvironment<H> {
  
  public void execute(ITransactionCall<H> transaction) throws Throwable;
  
  @SuppressWarnings("unchecked")
  public default <R> R calculate(ITransactionCalc<H, R> transaction) {
    Object[] result = new Object[1];
    try {
      execute(h -> result[0] = transaction.execute(h));
    } catch (Throwable e) {
      new RuntimeException(e);
    }
    return (R)result[0];
  }
  
}
