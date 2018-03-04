package rocks.inspectit.jaeger.bt.model.trace.config;

public class Configuration {
    private String database;
    private String serviceName;
    private long startTime;
    private long endTime;
    private long interval;
    private KafkaConfig kafka;
    private ElasticSearchConfig elasticsearch;
    private CassandraConfig cassandra;

    public KafkaConfig getKafkaConfig() {
        return kafka;
    }

    public void setKafka(KafkaConfig kafkaConfig) {
        this.kafka = kafkaConfig;
    }

    public ElasticSearchConfig getElasticSearchConfig() {
        return elasticsearch;
    }

    public void setElasticsearch(ElasticSearchConfig elasticSearchConfig) {
        this.elasticsearch = elasticSearchConfig;
    }

    public CassandraConfig getCassandraConfig() {
        return cassandra;
    }

    public void setCassandra(CassandraConfig cassandraConfig) {
        this.cassandra = cassandraConfig;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }
}
