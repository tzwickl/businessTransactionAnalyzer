package rocks.inspectit.jaeger.bt.analyzer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rocks.inspectit.jaeger.bt.model.trace.cassandra.Trace;
import rocks.inspectit.jaeger.bt.model.trace.cassandra.TraceKeyValue;

import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class TracesAnalyzerCassandra implements TracesAnalyzer {
    private static final Logger logger = LoggerFactory.getLogger(TracesAnalyzerCassandra.class);
    private final static String BT = "business_transaction";
    private final List<Trace> traces;
    private final Map<Long, Trace> spans;

    public TracesAnalyzerCassandra(final List<Trace> traces) {
        this.spans = new HashMap<>();
        this.traces = traces;
    }

    public void findBusinessTransactions() {
        final Set<String> businessTransactions = new HashSet<>();

        this.traces.forEach(trace -> {
            spans.put(trace.getSpanId(), trace);
        });

        this.traces.forEach(trace -> {
            String businessTransactionName = this.findBusinessTransactionName(trace);
            businessTransactions.add(businessTransactionName);
            this.setBusinessTransaction(trace, businessTransactionName);
        });

        logger.info("Detected business transactions: " + Arrays.deepToString(businessTransactions.toArray()));
    }

    public Long getLatestTimestamp() {
        return this.traces.stream().max(Comparator.comparing(Trace::getStartTime)).get().getStartTime();
    }

    private String findBusinessTransactionName(Trace trace) {
        if (trace.getParentId() != 0) {
            return trace.getOperationName();
        } else {
            Trace parentTrace = this.getParentSpan(trace);
            if (parentTrace == null) {
                return null;
            }
            return this.findBusinessTransactionName(parentTrace);
        }
    }

    private Trace getParentSpan(Trace childTrace) {
        if (!this.spans.containsKey(childTrace.getParentId())) {
            logger.warn("No parent span with ID " + childTrace.getParentId()
                    + " found for child span with ID " + childTrace.getSpanId());
            return null;
        } else {
            return this.spans.get(childTrace.getParentId());
        }
    }

    private void setBusinessTransaction(Trace trace, String businessTransaction) {
        List<TraceKeyValue> tags = trace.getTags();

        TraceKeyValue businessTransactionTag = new TraceKeyValue();
        businessTransactionTag.setKey(BT);
        businessTransactionTag.setValueType("string");
        businessTransactionTag.setValueString(businessTransaction);

        if (tags == null) {
            tags = new ArrayList<>();
            tags.add(businessTransactionTag);
            trace.setTags(tags);
        } else {
            Stream<TraceKeyValue> foundTags = tags.stream().filter(tag -> {
                return tag.getKey().equals(BT);
            });
            List<TraceKeyValue> matchedTags = foundTags.collect(toList());
            if (matchedTags.size() > 0) {
                matchedTags.forEach(tag -> {
                    tag.setValueString(businessTransaction);
                });
            } else {
                tags.add(businessTransactionTag);
            }
        }
    }
}
