package com.feib;


import com.fasterxml.jackson.databind.ObjectMapper;


import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

public class ElasticsearchDataExtractor {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchDataExtractor.class.getName());

    public static void main(String[] args) {
        // Configure Elasticsearch client
        String elasticsearchHost = "X.x.x.x";//replace real ip
        int elasticsearchPort = 9200;
        String elasticsearchScheme = "http";
        String username = "xxxx";
        String password = "xxxx";

        try {
            // Create a CredentialsProvider
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password)
            );

            try (RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(elasticsearchHost, elasticsearchPort, elasticsearchScheme))
                            .setHttpClientConfigCallback(httpClientBuilder ->
                                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
            )) {

                // Define the index pattern (SIT-All-Biz)
                String indexPattern = "api_gateway_log*,eai-flow-*,biz-service-*,feib-schedule-service-*";

                // Define query range
                String gteTimestamp = "2025-01-05T13:44:23.378";
                String lteTimestamp = "2025-01-08T13:44:23.378";

                QueryBuilder queryRange = QueryBuilders.rangeQuery("timestamp")
                        .gte(gteTimestamp)
                        .lte(lteTimestamp);

                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                        .query(queryRange)
                        .size(100); // Set the limit for response size

                // Execute the query
                SearchRequest searchRequest = new SearchRequest(indexPattern);
                searchRequest.source(searchSourceBuilder);

                SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

                // Collect the documents
                List<Map<String, Object>> documents = Arrays.stream(response.getHits().getHits())
                        .map(SearchHit::getSourceAsMap)
                        .collect(Collectors.toList());

                // Flatten the documents & ensure all values are strings
                List<Map<String, Object>> flattenedDocuments = documents.stream()
                        .map(ElasticsearchDataExtractor::flattenAndConvertToStrings)
                        .collect(Collectors.toList());

                logger.info("Found {} documents", flattenedDocuments.size());

                // Serialize to JSON
                ObjectMapper objectMapper = new ObjectMapper();
                String jsonData = objectMapper.writeValueAsString(flattenedDocuments);

                // Split content into chunks of 512 characters
                List<String> chunks = splitContent(jsonData, 512);

                // Write to JSON file
                String outputFilename = "D:\\\\EsDump\\\\knowledge_base_data2_" + getCurrentTimestamp() + ".json";
                try (FileWriter writer = new FileWriter(outputFilename)) {
                    for (String chunk : chunks) {
                        writer.write(chunk + "\n");
                    }
                }

                logger.info("File has been saved as: {}", outputFilename);

            }
        } catch (IOException e) {
           logger.error("{}",e);
        }
    }

    // Helper function to flatten nested maps
    private static Map<String, Object> flattenAndConvertToStrings(Map<String, Object> map) {
        Map<String, Object> flattenedMap = new HashMap<>();

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nestedMap = (Map<String, Object>) entry.getValue();
                Map<String, Object> flattenedNestedMap = flattenAndConvertToStrings(nestedMap);
                for (Map.Entry<String, Object> nestedEntry : flattenedNestedMap.entrySet()) {
                    flattenedMap.put(entry.getKey() + "." + nestedEntry.getKey(), nestedEntry.getValue().toString());
                }
            } else {
                flattenedMap.put(entry.getKey(), Objects.toString(entry.getValue(), ""));
            }
        }

        return flattenedMap;
    }

    // Helper function to split string content into chunks
    private static List<String> splitContent(String content, int chunkSize) {
        List<String> chunks = new ArrayList<>();
        for (int i = 0; i < content.length(); i += chunkSize) {
            chunks.add(content.substring(i, Math.min(i + chunkSize, content.length())));
        }
        return chunks;
    }

    // Helper function to get the current timestamp
    private static String getCurrentTimestamp() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
        return LocalDateTime.now().format(formatter);
    }
}