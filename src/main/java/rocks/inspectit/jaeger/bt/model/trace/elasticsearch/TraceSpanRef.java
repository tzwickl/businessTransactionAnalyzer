package rocks.inspectit.jaeger.bt.model.trace.elasticsearch;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class TraceSpanRef {
    @JsonProperty("refType")
    String refType;
    @JsonProperty("traceID")
    String traceId;
    @JsonProperty("spanID")
    Long spanId;
}
