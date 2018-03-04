package rocks.inspectit.jaeger.bt.model.trace.config;

public class CassandraConfig {
    private String host;
    private String keyspace;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }
}
