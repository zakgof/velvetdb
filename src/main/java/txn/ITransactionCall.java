package txn;

@FunctionalInterface
public interface ITransactionCall<H> {

  public void execute(H handle) throws Throwable;

}
