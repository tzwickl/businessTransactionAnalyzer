package rocks.inspectit.jaeger.bt.connectors.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rocks.inspectit.jaeger.bt.connectors.Constants;
import rocks.inspectit.jaeger.bt.connectors.IDatabase;
import rocks.inspectit.jaeger.bt.model.trace.elasticsearch.Trace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Elasticsearch implements IDatabase<Trace> {
    private static final Logger logger = LoggerFactory.getLogger(Elasticsearch.class);

    private final RestHighLevelClient client;
    private final String doc;
    private final Map<String, String> ids;
    private final Map<String, String> types;
    private final ObjectMapper objectMapper;

    public Elasticsearch(final String host, final String doc) {
        this.doc = doc;
        this.ids = new HashMap<>();
        this.types = new HashMap<>();
        this.objectMapper = new ObjectMapper();
        this.client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, 9200, "http")));
    }

    @Override
    public void closeConnection() throws IOException {
        client.close();
    }

    @Override
    public List<Trace> getTraces(final String serviceName) {
        SearchRequest searchRequest = new SearchRequest(this.doc);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(Constants.SERVICE_NAME_PATH.getValue(), serviceName));
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(1L));

        List<Trace> traces = new ArrayList<>();
        try {
            SearchResponse searchResponse = this.client.search(searchRequest);
            SearchHits hits = searchResponse.getHits();
            String scrollId = searchResponse.getScrollId();

            while (hits.getHits().length > 0) {
                for (SearchHit hit : hits) {
                    Trace trace = this.objectMapper.readValue(hit.getSourceAsString(), Trace.class);
                    traces.add(trace);
                    this.ids.put(trace.getSpanId(), hit.getId());
                    this.types.put(trace.getSpanId(), hit.getType());
                }
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueMinutes(1L));
                searchResponse = this.client.searchScroll(scrollRequest);
                scrollId = searchResponse.getScrollId();
                hits = searchResponse.getHits();
            }

        } catch (IOException | ElasticsearchException e) {
            e.printStackTrace();
        }

        return traces;
    }

    @Override
    public List<Trace> getTraces(final String serviceName, Long startTime) {
        return null;
    }

    @Override
    public List<Trace> getTraces(final String serviceName, Long startTime, Long endTime) {
        return null;
    }

    @Override
    public void saveTraces(List<Trace> traces) {
        BulkRequest request = new BulkRequest();

        traces.forEach(trace -> {
            request.add(this.createUpdateRequest(trace));
        });

        try {
            BulkResponse bulkResponse = client.bulk(request);
            if (bulkResponse.hasFailures()) {
                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                    if (bulkItemResponse.isFailed()) {
                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                        logger.error("Failure while updating trace: "  + failure.toString());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private UpdateRequest createUpdateRequest(Trace trace) {
        try {
            UpdateRequest request = new UpdateRequest(this.doc, this.types.get(trace.getSpanId()), this.ids.get(trace.getSpanId()));
            String json = this.objectMapper.writeValueAsString(trace);
            request.doc(json, XContentType.JSON);
            return request;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
