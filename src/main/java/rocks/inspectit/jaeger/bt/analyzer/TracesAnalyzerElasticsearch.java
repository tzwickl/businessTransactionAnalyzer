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
    private final List<Trace> traces;

    public TracesAnalyzerElasticsearch(final List<Trace> traces) {
        this.traces = traces;
    }

    public void findBusinessTransactions() {
        final Set<String> businessTransactions = new HashSet<>();
        Map<String, List<Trace>> groupedTraces = new HashMap<>();

        this.traces.forEach(trace -> {
            if (!groupedTraces.containsKey(trace.getTraceId())) {
                groupedTraces.put(trace.getTraceId(), new ArrayList<Trace>());
            }
            groupedTraces.get(trace.getTraceId()).add(trace);
        });

        groupedTraces.forEach((spanId, span) -> {
            String businessTransactionName = this.findBusinessTransactionName(span);
            businessTransactions.add(businessTransactionName);
            this.setBusinessTransaction(span, businessTransactionName);
        });

        logger.info("Detected business transactions: " + Arrays.deepToString(businessTransactions.toArray()));
    }

    public Long getLatestTimestamp() {
        return this.traces.stream().max(Comparator.comparing(Trace::getStartTime)).get().getStartTime();
    }

    private String findBusinessTransactionName(List<Trace> span) {
        if (span.isEmpty()) {
            return "";
        }
        for (int i = 0; i < span.size(); i++) {
            if (span.get(i).getParentId().equals("0")) {
                return span.get(i).getOperationName();
            }
        }
        return span.get(0).getOperationName();
    }

    private void setBusinessTransaction(List<Trace> span, String businessTransaction) {
        for (int i = 0; i < span.size(); i++) {
            List<TraceKeyValue> tags = span.get(i).getTags();

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
}
