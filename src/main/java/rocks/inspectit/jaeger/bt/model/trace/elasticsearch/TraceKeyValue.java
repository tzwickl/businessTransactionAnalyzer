package rocks.inspectit.jaeger.bt.model.trace.elasticsearch;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class TraceKeyValue {
    @JsonProperty("key")
    String key;
    @JsonProperty("type")
    String type;
    @JsonProperty("value")
    String value;
}
