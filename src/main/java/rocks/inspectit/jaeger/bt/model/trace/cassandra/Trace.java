package rocks.inspectit.jaeger.bt.model.trace.cassandra;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.Table;
import lombok.Data;

import java.nio.ByteBuffer;
import java.util.List;

@Data
@Table(name = "traces")
public class Trace {
    @Column(name = "trace_id")
    ByteBuffer traceId;
    @Column(name = "span_id")
    Long spanId;
    @Column(name = "span_hash")
    Long spanHash;
    @Column(name = "duration")
    Long duration;
    @Column(name = "flags")
    Integer flags;
    @Column(name = "logs")
    @Frozen
    List<TraceLog> logs;
    @Column(name = "operation_name")
    String operationName;
    @Column(name = "parent_id")
    Long parentId;
    @Column(name = "process")
    TraceProcess process;
    @Column(name = "refs")
    @Frozen
    List<TraceSpanRef> refs;
    @Column(name = "start_time")
    Long startTime;
    @Column(name = "tags")
    @Frozen
    List<TraceKeyValue> tags;
}
