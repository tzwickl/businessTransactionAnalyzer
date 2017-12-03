package rocks.inspectit.jaeger.bt.model.trace;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import lombok.Data;

import java.nio.ByteBuffer;


@Data
@UDT(name = "keyvalue")
public class TraceKeyValue {
    @Field(name = "key")
    String key;
    @Field(name = "value_type")
    String valueType;
    @Field(name = "value_string")
    String valueString;
    @Field(name = "value_bool")
    Boolean valueBool;
    @Field(name = "value_long")
    Long valueLong;
    @Field(name = "value_double")
    Double valueDouble;
    @Field(name = "value_binary")
    ByteBuffer valueBinary;
}
