package rocks.inspectit.jaeger.bt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rocks.inspectit.jaeger.bt.analyzer.TracesAnalyzer;
import rocks.inspectit.jaeger.bt.analyzer.TracesAnalyzerElasticsearch;
import rocks.inspectit.jaeger.bt.connectors.IDatabase;
import rocks.inspectit.jaeger.bt.connectors.cassandra.Cassandra;
import rocks.inspectit.jaeger.bt.connectors.elasticsearch.Elasticsearch;
import rocks.inspectit.jaeger.bt.connectors.kafka.Kafka;
import rocks.inspectit.jaeger.bt.model.trace.config.Configuration;
import rocks.inspectit.jaeger.bt.model.trace.elasticsearch.Trace;

import java.util.List;

public class Analyzer {
    private static final Logger logger = LoggerFactory.getLogger(Analyzer.class);

    private static final String CASSANDRA = "cassandra";
    private static final String ELASTICSEARCH = "elasticsearch";
    private static final String KAFKA = "kafka";

    private final Configuration configuration;

    public Analyzer(Configuration configuration) {
        this.configuration = configuration;
    }

    public int start() {
        IDatabase database = null;

        switch (configuration.getDatabase()) {
            case CASSANDRA:
                database = new Cassandra(configuration.getCassandraConfig());
                logger.info("Using database " + CASSANDRA);
                break;
            case ELASTICSEARCH:
                database = new Elasticsearch(configuration.getElasticSearchConfig());
                logger.info("Using database " + ELASTICSEARCH);
                break;
            case KAFKA:
                database = new Kafka(configuration.getServiceName(), configuration.getKafkaConfig());
                logger.info("Using database " + KAFKA);
                break;
            default:
                logger.error(configuration.getDatabase() + " is not a known database!");
                return 1;
        }

        Long startTime = null;
        Long endTime = null;
        try {
            startTime = configuration.getStartTime() * 1000000;
            endTime = configuration.getEndTime() * 1000000;
        } catch (NumberFormatException e) {
            // Do nothing
        }

        Long follow = null;
        try {
            follow = configuration.getInterval() * 1000;
        } catch (NumberFormatException e) {
            // Do nothing
        }

        if (follow == null) {
            run(database, configuration.getServiceName(), startTime, endTime);
        } else {
            if (startTime == null) {
                startTime = System.currentTimeMillis() * 1000;
                logger.info("Timestamp: " + startTime);
            }
            try {
                while (true) {
                    startTime = run(database, configuration.getServiceName(), startTime, null) + 1;
                    Thread.sleep(follow);
                }
            } catch (InterruptedException e) {
                // Do nothing
            }
        }

        return 0;
    }

    private Long run(IDatabase database, String serviceName, Long startTime, Long endTime) {
        logger.info("###############_START_###############");
        List<Trace> traces;

        if (startTime != null && endTime != null) {
            traces = database.getTraces(serviceName, startTime, endTime);
        } else if (startTime != null) {
            traces = database.getTraces(serviceName, startTime);
        } else {
            traces = database.getTraces(serviceName);
        }

        logger.info("Number of Traces to analyze: " + traces.size());

        if (traces.isEmpty()) {
            logger.warn("No Traces found to analyze!");
            logger.info("###############_END_#################");
            return startTime;
        }

        TracesAnalyzer tracesAnalyzer = new TracesAnalyzerElasticsearch(traces);
        tracesAnalyzer.findBusinessTransactions();

        logger.info("Finished analyzing Traces");

        database.saveTraces(traces);

        logger.info("Updated Traces in database");
        logger.info("###############_END_#################");

        return tracesAnalyzer.getLatestTimestamp();
    }
}
