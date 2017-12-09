package rocks.inspectit.jaeger.bt.model.trace.cassandra;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.UDT;
import lombok.Data;

import java.util.List;

@Data
@UDT(name = "log")
public class TraceLog {
    @Field(name = "ts")
    Long ts;
    @Field(name = "fields")
    @Frozen
    List<TraceKeyValue> fields;
}