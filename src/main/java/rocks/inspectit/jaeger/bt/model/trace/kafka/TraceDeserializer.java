package rocks.inspectit.jaeger.bt.model.trace.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import rocks.inspectit.jaeger.bt.model.trace.elasticsearch.Trace;

import java.util.Map;

public class TraceDeserializer implements Deserializer<Trace> {
    @Override public void close() {
    }

    @Override public void configure(Map<String, ?> arg0, boolean arg1) {
    }

    @Override
    public Trace deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        Trace trace = null;
        try {
            trace = mapper.readValue(arg1, Trace.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return trace;
    }
}
