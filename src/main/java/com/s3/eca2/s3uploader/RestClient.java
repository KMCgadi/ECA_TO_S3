//package com.s3.eca2.s3uploader;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.net.URI;
//import java.text.SimpleDateFormat;
//import java.util.*;
//import java.util.logging.Logger;
//import java.util.stream.Collectors;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.springframework.core.ParameterizedTypeReference;
//import org.springframework.http.*;
//import org.springframework.http.client.ClientHttpResponse;
//import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
//import org.springframework.util.LinkedMultiValueMap;
//import org.springframework.util.MultiValueMap;
//import org.springframework.web.client.DefaultResponseErrorHandler;
//import org.springframework.web.client.HttpClientErrorException;
//import org.springframework.web.client.HttpServerErrorException;
//import org.springframework.web.client.RestTemplate;
//
//
//public class RestClient {
//    private static final Logger logger = Logger.getLogger(S3Uploader.class.getName());
//    private RestTemplate restTemplate;
//    private MultiValueMap<String, String> headers = null;
//
//    private String schema;
//    private String host;
//    private Integer port;
//
//    private Map<String, Object> requestData = null;
//
//    public Map<String, Object> getRequestData() {
//        return requestData;
//    }
//
//    public void setRequestData(Map<String, Object> requestData) {
//        this.requestData = requestData;
//    }
//
//    public RestClient(String host) {
//        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
//        factory.setConnectTimeout(10 * 1000);
//        factory.setReadTimeout(10 * 1000);
//
//        this.restTemplate = new RestTemplate(factory);
//
//        this.headers = new LinkedMultiValueMap<>();
//        this.headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
//
//        this.host = host;
//        this.schema = "http";
//
//        restTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
//            @Override
//            public void handleError(ClientHttpResponse response) throws IOException {
//                HttpStatus statusCode = response.getStatusCode();
//                MediaType contentType = response.getHeaders().getContentType();
//                String statusText = response.getStatusText();
//                InputStream responseBody = response.getBody();
//                String bodyContent = new BufferedReader(new InputStreamReader(responseBody))
//                        .lines().collect(Collectors.joining("\n"));
//
//                // 로깅: 상태 코드, 상태 텍스트, 응답 본문
//                logger.info("Error response received: " + statusCode + " " + statusText);
//                logger.info("Error response body: " + bodyContent);
//
//                if (statusCode.series() == HttpStatus.Series.SERVER_ERROR) {
//                    // 서버 오류일 때
//                    logger.info("서버오류");
//                } else if (statusCode.series() == HttpStatus.Series.CLIENT_ERROR) {
//                    // 클라이언트 오류일 때
//                    logger.info("클라이언트 오류");
//                }
//
//                // 기본 오류 처리 호출
//                super.handleError(response);
//            }
//        });
//
//
//    }
//
//    public RestClient(String host, String schema, int intConnectionTimeout, int intReadTimeout) {
//
//        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
//        factory.setConnectTimeout(intConnectionTimeout * 1000);
//        factory.setReadTimeout(intReadTimeout * 1000);
//
//        this.restTemplate = new RestTemplate(factory);
//
//        this.headers = new LinkedMultiValueMap<>();
//        //this.headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
//
//        this.host = host;
//        this.schema = schema;
//
//        restTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
//            @Override
//            public void handleError(ClientHttpResponse response) throws IOException {
//                HttpStatus statusCode = response.getStatusCode();
//                MediaType contentType = response.getHeaders().getContentType();
//                String statusText = response.getStatusText();
//                InputStream responseBody = response.getBody();
//                String bodyContent = new BufferedReader(new InputStreamReader(responseBody))
//                        .lines().collect(Collectors.joining("\n"));
//
//                logger.info("Error response received: " + statusCode + " " + statusText);
//                logger.info("Error response body: " + bodyContent);
//
//                if (statusCode.series() == HttpStatus.Series.SERVER_ERROR) {
//                    logger.info("서버오류 "+bodyContent);
//                } else if (statusCode.series() == HttpStatus.Series.CLIENT_ERROR) {
//                    logger.info("클라오류 "+bodyContent);
//                }
//                super.handleError(response);
//            }
//        });
//    }
//
//    public String getSchema() {
//        return schema;
//    }
//
//    public void setSchema(String schema) {
//        this.schema = schema;
//    }
//
//    public String getHost() {
//        return host;
//    }
//
//    public void setHost(String host) {
//        this.host = host;
//    }
//
//    public int getPort() {
//        return port;
//    }
//
//    public void setPort(int port) {
//        this.port = port;
//    }
//
//    public RestTemplate getRestTemplate() {
//        return this.restTemplate;
//    }
//
//    public void clearHeaders() {
//        this.headers.clear();
//    }
//
//    public void addHeader(String key, String value) {
//        this.headers.add(key, value);
//    }
//
//    private URI createURI(String path) throws Exception {
//        if (path != null && !"/".equals(path.substring(0, 1)))
//            path = "/" + path;
//
//        if (port == null)
//            return new URI(schema + "://" + host + path);
//
//        return new URI(schema + "://" + host + ":" + port + path);
//    }
//
//    // get
//    public <T> ResponseEntity<T> getForEntity(String path, Class<T> responseType) throws Exception {
//        return execute(path, HttpMethod.GET, null, responseType);
//    }
//
//    public <T> T get(String path, Class<T> responseType) throws Exception {
//        return getForEntity(path, responseType).getBody();
//    }
//
//    public <T> ResponseEntity<T> getForEntity(String path, ParameterizedTypeReference<T> responseType) throws Exception {
//        return execute(path, HttpMethod.GET, null, responseType);
//    }
//
//    public <T> T get(String path, ParameterizedTypeReference<T> responseType) throws Exception {
//        return getForEntity(path, responseType).getBody();
//    }
//
//    // post
//    public <T> ResponseEntity<T> postForEntity(String path, Object body, Class<T> responseType) throws Exception {
//        return execute(path, HttpMethod.POST, body, responseType);
//    }
//
//    public <T> T post(String path, Object body, Class<T> responseType) throws Exception {
//        return postForEntity(path, body, responseType).getBody();
//    }
//
//    public <T> ResponseEntity<T> postForEntity(String path, Object body, ParameterizedTypeReference<T> responseType) throws Exception {
//        return execute(path, HttpMethod.POST, body, responseType);
//    }
//
//    public <T> T post(String path, Object body, ParameterizedTypeReference<T> responseType) throws Exception {
//        return postForEntity(path, body, responseType).getBody();
//    }
//
//    // put
//    public <T> ResponseEntity<T> putForEntity(String path, Object body, Class<T> responseType) throws Exception {
//        return execute(path, HttpMethod.PUT, body, responseType);
//    }
//
//    public <T> T put(String path, Object body, Class<T> responseType) throws Exception {
//        return putForEntity(path, body, responseType).getBody();
//    }
//
//    public <T> ResponseEntity<T> putForEntity(String path, Object body, ParameterizedTypeReference<T> responseType) throws Exception {
//        return execute(path, HttpMethod.PUT, body, responseType);
//    }
//
//    public <T> T put(String path, Object body, ParameterizedTypeReference<T> responseType) throws Exception {
//        return putForEntity(path, body, responseType).getBody();
//    }
//
//    // delete
//    public <T> ResponseEntity<T> deleteForEntity(String path, Object body, Class<T> responseType) throws Exception {
//        return execute(path, HttpMethod.DELETE, body, responseType);
//    }
//
//    public <T> T delete(String path, Object body, Class<T> responseType) throws Exception {
//        return deleteForEntity(path, body, responseType).getBody();
//    }
//
//    public <T> ResponseEntity<T> deleteForEntity(String path, Object body, ParameterizedTypeReference<T> responseType) throws Exception {
//        return execute(path, HttpMethod.DELETE, body, responseType);
//    }
//
//    public <T> T delete(String path, Object body, ParameterizedTypeReference<T> responseType) throws Exception {
//        return deleteForEntity(path, body, responseType).getBody();
//    }
//
//    public <T> ResponseEntity<T> execute(String path, HttpMethod method, Object body, Class<T> responseType) throws Exception {
//        String startTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
//        logging(startTime, path, method, body);
//
//        ResponseEntity<T> responseEntity = restTemplate.exchange(new RequestEntity<>(body, headers, method, createURI(path)), responseType);
//        logging(startTime, path, method, body, responseEntity);
//
//        return responseEntity;
//    }
//
//    public <T> ResponseEntity<T> execute(String path, HttpMethod method, Object body, ParameterizedTypeReference<T> responseType) throws Exception {
//        ObjectMapper mapper = new ObjectMapper();
//        String jsonRequestBody = mapper.writeValueAsString(body);
//        logger.info("Request JSON: " + jsonRequestBody); //base64 디코딩하여 원문확인 가능 디버깅시 참고
//
//        try {
//            loggingRequest(path, method, body);
//            return restTemplate.exchange(new RequestEntity<>(body, headers, method, createURI(path)), responseType);
//        } catch (HttpClientErrorException | HttpServerErrorException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//
//    private void loggingRequest(String path, HttpMethod method, Object body) {
//        logger.info("Sending Request: " + method.toString() + " " + path);
//        logger.info("Request Body: " + (body == null ? "No Body" : body.toString()));
//        // 헤더 로깅 추가
//        headers.forEach((key, value) -> logger.info("Header: " + key + " = " + value));
//    }
//
//
//    private void logging(String startTime, String path, HttpMethod method, Object body) {
//        Map<String, Object> requestData = new HashMap<>();
//        requestData.put("startTime", startTime);
//        requestData.put("method", method);
//        requestData.put("path", path);
//        requestData.put("body", (body == null ? "empty" : body.toString()));
//        setRequestData(requestData);
//
//        logger.info("start request =============================================================================");
//        logger.info("start date : " + startTime);
//        logger.info("    method : " + method.toString());
//        logger.info("      path : " + path);
//        logger.info("      body : " + (body == null ? "empty" : body.toString()));
//        logger.info("end request   =============================================================================");
//    }
//
//    private void logging(String startTime, String path, HttpMethod method, Object body, ResponseEntity<?> response) {
//        String responseContent = (response != null && response.hasBody()) ? response.getBody().toString() : "No Body";
//        String endTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
//        logger.info("start response =============================================================================");
//        logger.info("start date : " + startTime);
//        logger.info("  end date : " + endTime);
//        logger.info("    method : " + method.toString());
//        logger.info("      path : " + path);
//        logger.info("      body : " + (body == null ? "empty" : body.toString()));
//        logger.info("  response : " + (response == null ? "empty" : response.toString()));
//        logger.info("  response : " + responseContent);
//        logger.info("end response   =============================================================================");
//    }
//
//    // patch
//    public <T> ResponseEntity<T> patchForEntity(String path, Object body, Class<T> responseType) throws Exception {
//        return execute(path, HttpMethod.PATCH, body, responseType);
//    }
//
//    public <T> T patch(String path, Object body, Class<T> responseType) throws Exception {
//        return deleteForEntity(path, body, responseType).getBody();
//    }
//
//    public <T> ResponseEntity<T> patchForEntity(String path, Object body, ParameterizedTypeReference<T> responseType) throws Exception {
//        return execute(path, HttpMethod.PATCH, body, responseType);
//    }
//
//    public <T> T patch(String path, Object body, ParameterizedTypeReference<T> responseType) throws Exception {
//        return patchForEntity(path, body, responseType).getBody();
//    }
//}
