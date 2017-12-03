package rocks.inspectit.jaeger.bt.model.trace;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@UDT(name = "span_ref")
public class TraceSpanRef {
    @Field(name = "ref_type")
    String refType;
    @Field(name = "trace_id")
    ByteBuffer traceId;
    @Field(name = "span_id")
    Long spanId;
}
