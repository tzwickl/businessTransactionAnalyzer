package rocks.inspectit.jaeger.bt.analyzer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rocks.inspectit.jaeger.model.trace.kafka.Trace;
import rocks.inspectit.jaeger.model.trace.kafka.TraceKeyValue;

import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static rocks.inspectit.jaeger.model.Constants.BT_TAG;

public class TracesAnalyzerElasticsearch implements TracesAnalyzer {
    private static final Logger logger = LoggerFactory.getLogger(TracesAnalyzerElasticsearch.class);
    final Map<String, Trace> spans;
    private final List<Trace> traces;

    public TracesAnalyzerElasticsearch(final List<Trace> traces) {
        this.spans = new HashMap<>();
        this.traces = traces;
    }

    public void findBusinessTransactions() {
        final Set<String> businessTransactions = new HashSet<>();

        this.traces.forEach(trace -> {
            spans.put(trace.getTraceId(), trace);
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
        if (trace.getParentId().equals("0")) {
            return trace.getOperationName();
        } else if (trace.getTraceId().equals(trace.getParentId())) {
            return trace.getOperationName();
        } else {
            Trace parentTrace = this.getParentSpan(trace);
            if (parentTrace == null) {
                return "";
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
        businessTransactionTag.setKey(BT_TAG);
        businessTransactionTag.setType("string");
        businessTransactionTag.setValue(businessTransaction);

        if (tags.isEmpty()) {
            tags.add(businessTransactionTag);
        } else {
            Stream<TraceKeyValue> foundTags = tags.stream().filter(tag -> {
                return tag.getKey().equals(BT_TAG);
            });
            List<TraceKeyValue> matchedTags = foundTags.collect(toList());
            if (matchedTags.size() > 0) {
                matchedTags.forEach(tag -> {
                    tag.setValue(businessTransaction);
                });
            } else {
                tags.add(businessTransactionTag);
            }
        }
    }
}
