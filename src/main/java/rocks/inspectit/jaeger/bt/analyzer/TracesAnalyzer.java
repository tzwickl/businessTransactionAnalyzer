package rocks.inspectit.jaeger.bt.analyzer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rocks.inspectit.jaeger.bt.model.trace.Trace;
import rocks.inspectit.jaeger.bt.model.trace.TraceKeyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class TracesAnalyzer {
    private static final Logger logger = LoggerFactory.getLogger(TracesAnalyzer.class);

    private final List<Trace> traces;
    private final static String BT = "business_transaction";

    public TracesAnalyzer(final List<Trace> traces) {
        this.traces = traces;
    }

    public void findBusinessTransactions() {
        final Map<Long, Trace> spans = new HashMap<>();
        final List<String> businessTransactions = new ArrayList<>();

        this.traces.forEach(trace -> {
            spans.put(trace.getSpanId(), trace);
        });

        this.traces.forEach(trace -> {
            if (trace.getParentId() != 0) {
                final Trace parent = spans.get(trace.getParentId());
                this.setBusinessTransaction(trace, parent.getOperationName());
            } else {
                businessTransactions.add(trace.getOperationName());
                this.setBusinessTransaction(trace, trace.getOperationName());
            }
        });

        logger.info("Detected business transactions: " + businessTransactions);
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
