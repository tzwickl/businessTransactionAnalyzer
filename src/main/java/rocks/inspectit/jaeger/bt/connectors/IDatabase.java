package rocks.inspectit.jaeger.bt.connectors;

import rocks.inspectit.jaeger.bt.model.trace.Trace;

import java.util.List;

public interface IDatabase {
    void closeConnection();

    List<Trace> getTraces();
    List<Trace> getTraces(Long startTime);
    List<Trace> getTraces(Long startTime, Long endTime);
    void saveTraces(List<Trace> traces);
}
