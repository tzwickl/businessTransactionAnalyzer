package rocks.inspectit.jaeger.bt.connectors;

public enum Constants {
    TRACES("traces"),
    START_TIME("start_time"),
    SERVICE_NAME_PATH("process.serviceName");

    private String value;

    Constants(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }
}
