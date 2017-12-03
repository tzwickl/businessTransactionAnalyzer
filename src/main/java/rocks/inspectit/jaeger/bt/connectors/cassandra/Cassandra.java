package rocks.inspectit.jaeger.bt.connectors.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rocks.inspectit.jaeger.bt.connectors.Constants;
import rocks.inspectit.jaeger.bt.connectors.IDatabase;
import rocks.inspectit.jaeger.bt.model.trace.Trace;

import java.util.List;

public class Cassandra implements IDatabase {
    private static final Logger logger = LoggerFactory.getLogger(Cassandra.class);

    private Cluster cluster;
    private Session session;
    private String keyspace;

    // Mappers
    MappingManager manager;
    Mapper<Trace> tracesMapper;

    public Cassandra(final String host, final String keyspace) {
        this.keyspace = keyspace;
        this.cluster = Cluster.builder().addContactPoint(host).build();
        this.session = this.cluster.connect(keyspace);
        this.manager = new MappingManager(session);
        this.tracesMapper = manager.mapper(Trace.class);
    }

    @Override
    public List<Trace> getTraces() {
        Statement query = QueryBuilder.select().from(Constants.TRACES.getValue());

        Result<Trace> traces = this.tracesMapper.map(this.session.execute(query));

        return traces.all();
    }

    @Override
    public List<Trace> getTraces(Long startTime) {
        Statement query = QueryBuilder.select().from(Constants.TRACES.getValue())
                .where(QueryBuilder.gt(Constants.START_TIME.getValue(), startTime))
                .allowFiltering();

        Result<Trace> traces = this.tracesMapper.map(this.session.execute(query));

        return traces.all();
    }

    @Override
    public List<Trace> getTraces(Long startTime, Long endTime) {
        Statement query = QueryBuilder.select().from(Constants.TRACES.getValue())
                .where(QueryBuilder.gt(Constants.START_TIME.getValue(), startTime))
                .and(QueryBuilder.lt(Constants.START_TIME.getValue(), endTime))
                .allowFiltering();

        Result<Trace> traces = this.tracesMapper.map(this.session.execute(query));

        return traces.all();
    }

    @Override
    public void saveTraces(List<Trace> traces) {
        traces.forEach(trace -> {
            this.tracesMapper.save(trace);
        });
    }

    @Override
    public void closeConnection() {
        this.cluster.close();
    }
}
