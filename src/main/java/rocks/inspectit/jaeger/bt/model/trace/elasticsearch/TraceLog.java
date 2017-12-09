package rocks.inspectit.jaeger.bt.model.trace.elasticsearch;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class TraceLog {
    @JsonProperty("timestamp")
    Long timestamp;
    @JsonProperty("fields")
    List<TraceKeyValue> fields;
}