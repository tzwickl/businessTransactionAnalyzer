package rocks.inspectit.jaeger.bt.model.trace.cassandra;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.UDT;
import lombok.Data;

import java.util.List;

@Data
@UDT(name = "process")
public class TraceProcess {
    @Field(name = "service_name")
    String serviceName;
    @Field(name = "tags")
    @Frozen
    List<TraceKeyValue> tags;
}
