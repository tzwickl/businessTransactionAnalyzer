package rocks.inspectit.jaeger.bt;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rocks.inspectit.jaeger.bt.analyzer.TracesAnalyzer;
import rocks.inspectit.jaeger.bt.connectors.IDatabase;
import rocks.inspectit.jaeger.bt.connectors.cassandra.Cassandra;
import rocks.inspectit.jaeger.bt.model.trace.Trace;

import java.util.ArrayList;
import java.util.List;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Cassandra.class);

    private static final String DB_HOST = "host";
    private static final String DB_KEYSPACE = "keyspace";
    private static final String SERVICE_NAME = "service";
    private static final String START_TIME = "start";
    private static final String END_TIME = "end";

    public static void main(String[] args) {
        final CommandLine cmd = parseArguments(args);
        String dbHost = cmd.getOptionValue(DB_HOST);
        String dbKeyspace = cmd.getOptionValue(DB_KEYSPACE);
        String serviceName = cmd.getOptionValue(SERVICE_NAME);
        Long startTime = null;
        Long endTime = null;
        try {
            startTime = Long.parseLong(cmd.getOptionValue(START_TIME));
            endTime = Long.parseLong(cmd.getOptionValue(END_TIME));
        } catch (NumberFormatException e) {

        }

        IDatabase database = new Cassandra(dbHost, dbKeyspace);

        List<Trace> traces;

        if (startTime != null && endTime != null) {
            traces = database.getTraces(startTime, endTime);
        } else if (startTime != null) {
            traces = database.getTraces(startTime);
        } else {
            traces = database.getTraces();
        }

        logger.info("Number of traces fetched: " + traces.size());

        List<Trace> tracesToAnalyze = new ArrayList<>();

        traces.forEach(trace -> {
            if (trace.getProcess().getServiceName().equals(serviceName)) {
                tracesToAnalyze.add(trace);
            }
        });

        logger.info("Number of Traces to analyze: " + tracesToAnalyze.size());

        TracesAnalyzer tracesAnalyzer = new TracesAnalyzer(tracesToAnalyze);
        tracesAnalyzer.findBusinessTransactions();

        logger.info("Finished analyzing Traces");

        database.saveTraces(tracesToAnalyze);

        logger.info("Updated Traces in database");

        System.exit(0);
    }

    private static CommandLine parseArguments(String[] args) {
        Options options = new Options();

        Option host = new Option("h", DB_HOST, true, "The database host");
        host.setRequired(true);
        options.addOption(host);

        Option keyspace = new Option("k", DB_KEYSPACE, true, "The database keyspace name");
        keyspace.setRequired(true);
        options.addOption(keyspace);

        Option service = new Option("s", SERVICE_NAME, true, "The service name to validate");
        service.setRequired(true);
        options.addOption(service);

        Option startTime = new Option("b", START_TIME, true, "The start time");
        startTime.setRequired(false);
        options.addOption(startTime);

        Option endTime = new Option("e", END_TIME, true, "The end time");
        endTime.setRequired(false);
        options.addOption(endTime);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
             return parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("jaegerBusinessTransactionDetector", options);

            System.exit(1);
            return null;
        }
    }
}
