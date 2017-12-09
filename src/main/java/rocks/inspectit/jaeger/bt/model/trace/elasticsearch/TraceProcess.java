package rocks.inspectit.jaeger.bt.model.trace.elasticsearch;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class TraceProcess {
    @JsonProperty("serviceName")
    String serviceName;
    @JsonProperty("tags")
    List<TraceKeyValue> tags;
}
