package rocks.inspectit.jaeger.bt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rocks.inspectit.jaeger.bt.analyzer.TracesAnalyzer;
import rocks.inspectit.jaeger.bt.analyzer.TracesAnalyzerElasticsearch;
import rocks.inspectit.jaeger.connectors.IDatasource;
import rocks.inspectit.jaeger.connectors.cassandra.Cassandra;
import rocks.inspectit.jaeger.connectors.elasticsearch.Elasticsearch;
import rocks.inspectit.jaeger.connectors.kafka.Kafka;
import rocks.inspectit.jaeger.model.config.Configuration;
import rocks.inspectit.jaeger.model.trace.kafka.Trace;

import java.util.List;

import static rocks.inspectit.jaeger.model.config.CassandraConfig.CASSANDRA;
import static rocks.inspectit.jaeger.model.config.ElasticSearchConfig.ELASTICSEARCH;
import static rocks.inspectit.jaeger.model.config.KafkaConfig.KAFKA;

public class Analyzer {
    private static final Logger logger = LoggerFactory.getLogger(Analyzer.class);

    private final Configuration configuration;

    public Analyzer(Configuration configuration) {
        this.configuration = configuration;
    }

    public int start() {
        IDatasource datasource = null;

        switch (configuration.getInput()) {
            case CASSANDRA:
                datasource = new Cassandra(configuration.getCassandra());
                logger.info("Using datasource " + CASSANDRA);
                break;
            case ELASTICSEARCH:
                datasource = new Elasticsearch(configuration.getElasticsearch());
                logger.info("Using datasource " + ELASTICSEARCH);
                break;
            case KAFKA:
                datasource = new Kafka(configuration.getServiceName(), configuration.getKafka());
                logger.info("Using datasource " + KAFKA);
                break;
            default:
                logger.error(configuration.getInput() + " is not a known datasource!");
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
            run(datasource, configuration.getServiceName(), startTime, endTime);
        } else {
            if (startTime == null) {
                startTime = System.currentTimeMillis() * 1000;
                logger.info("Timestamp: " + startTime);
            }
            try {
                while (true) {
                    startTime = run(datasource, configuration.getServiceName(), startTime, null) + 1;
                    Thread.sleep(follow);
                }
            } catch (InterruptedException e) {
                // Do nothing
            }
        }

        return 0;
    }

    private Long run(IDatasource database, String serviceName, Long startTime, Long endTime) {
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

        database.updateTraces(traces);

        logger.info("Updated Traces in database");
        logger.info("###############_END_#################");

        return tracesAnalyzer.getLatestTimestamp();
    }
}
