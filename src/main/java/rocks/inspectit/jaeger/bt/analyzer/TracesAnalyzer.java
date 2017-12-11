package rocks.inspectit.jaeger.bt.analyzer;

public interface TracesAnalyzer {
    void findBusinessTransactions();
    Long getLatestTimestamp();
}
