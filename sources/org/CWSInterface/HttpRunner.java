package org.CWSInterface;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Lädt eine Szenario-JSON und bietet Methoden, um die darin beschriebenen HTTP-Requests auszuführen.
 * REST: Java 11+ HttpClient
 * JSON: Jackson (nur jackson-databind, jackson-core, jackson-annotations)
 */
public class HttpRunner {

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient client = HttpClient.newHttpClient();

    // Datenstruktur für jeden Schritt
    static class Step {
        final String method;
        final String url;
        final JsonNode body;
        Step(String method, String url, JsonNode body) {
            this.method = method;
            this.url = url;
            this.body = body;
        }
    }

    // Alle geladenen Schritte
    private Step registerSchedulerStep;
    public List<Step> createNodesSteps = new ArrayList<>();
    private Step dagVerticesStep;
    private Step dagEdgesStep;
    private Step startBatchStep;
    private List<Step> registerTasksSteps = new ArrayList<>();
    private Step registerTasksBatchStep;
    private List<Step> registerOutputFilesStep = new ArrayList<>();
    private Step endBatchStep;
    private Step killExecutionStep;
    private Step resetCluster;
    private Step envelopedBatchStep;
    private Step reportCompletedTasksStep;
    private String dns;

    // Konstruktor: lädt und parst die JSON-Datei
    public HttpRunner(String jsonPath) throws Exception {
        JsonNode root = mapper.readTree(new File(jsonPath));
        JsonNode steps = root.path("steps");

        registerSchedulerStep = parseStep(steps.path("registerScheduler"));
        dns = registerSchedulerStep.body.path("dns").asText();
        for (JsonNode nodeStep : steps.path("createNodes")) {
            createNodesSteps.add(parseStep(nodeStep));
        }
        dagVerticesStep = parseStep(steps.path("submitDAG").path("vertices"));
        dagEdgesStep = parseStep(steps.path("submitDAG").path("edges"));
        startBatchStep = parseStep(steps.path("startBatch"));
        reportCompletedTasksStep = parseStep(steps.path("reportCompletedTasks"));
        for (JsonNode nodeStep : steps.path("registerOutputFiles")) {
            registerOutputFilesStep.add(parseStep(nodeStep));
        }
        // registerTasks can now be a single object (POST /tasks with array body) instead of an array of individual requests
        JsonNode registerTasksNode = steps.get("registerTasks");
        if (registerTasksNode != null && !registerTasksNode.isMissingNode() && !registerTasksNode.isNull()) {
            if (registerTasksNode.isArray()) {
                for (JsonNode regTask : registerTasksNode) {
                    registerTasksSteps.add(parseStep(regTask));
                }
            } else {
                registerTasksBatchStep = parseStep(registerTasksNode);
            }
        }
        endBatchStep = parseStep(steps.path("endBatch"));
        killExecutionStep = parseStep(steps.path("killExecution"));
        resetCluster = parseStep(steps.path("resetCluster"));
    }

    private Step parseStep(JsonNode node) {
        String method = node.path("method").asText();
        String url = node.path("url").asText();
        JsonNode body = node.get("body");
        return new Step(method, url, body);
    }

    // Führe einen HTTP-Request aus (POST/PUT/GET), body wird als JSON übertragen
    private String doRequest(Step step) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(step.url))
                .header("Content-Type", "application/json");

        String bodyStr = (step.body != null && !step.body.isNull()) ? step.body.toString() : "";
        switch (step.method.toUpperCase()) {
            case "POST":
                builder.POST(HttpRequest.BodyPublishers.ofString(bodyStr));
                break;
            case "PUT":
                builder.PUT(HttpRequest.BodyPublishers.ofString(bodyStr));
                break;
            case "GET":
                builder.GET();
                break;
            case "DELETE":
                builder.DELETE();
                break;
            default:
                throw new IllegalArgumentException("Unsupported HTTP method: " + step.method);
        }

        HttpResponse<String> resp = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        System.out.println("--> " + step.method + " " + step.url + ": " + resp.statusCode());
        return resp.body();
    }

    // Schritt-Funktionen
    public String registerScheduler() throws Exception {
        return doRequest(registerSchedulerStep);
    }
    public List<String> createNodes() throws Exception {
        List<String> out = new ArrayList<>();
        for (Step s : createNodesSteps) out.add(doRequest(s));
        return out;
    }
    public void submitDagVertices() throws Exception { doRequest(dagVerticesStep); }
    public void submitDagEdges() throws Exception { doRequest(dagEdgesStep); }
    public String startBatch() throws Exception { return doRequest(startBatchStep); }
    public String reportCompletedTasks() throws Exception { return reportCompletedTasksStep == null ? null : doRequest(reportCompletedTasksStep); }
    public List<String> registerOutputFiles() throws Exception {
        List<String> out = new ArrayList<>();
        for (Step s : registerOutputFilesStep) out.add(doRequest(s));
        return out;
    }
    public List<String> registerTasks() throws Exception {
        List<String> out = new ArrayList<>();
        for (Step s : registerTasksSteps) out.add(doRequest(s));
        return out;
    }
    public List<String> registerTasksSmart() throws Exception {
        if (registerTasksBatchStep != null) {
            List<String> out = new ArrayList<>();
            out.add(doRequest(registerTasksBatchStep));
            return out;
        }
        return registerTasks();
    }
    public String endBatch() throws Exception { return doRequest(endBatchStep); }

    /**
     * Fragt für jede Task-ID die zugewiesene Node ab.
     * Voraussetzung: Der Pod-Name entspricht dem Task-RunName.
     * @return Map: taskId → nodeName (oder null, falls nicht zugewiesen)
     * @throws Exception bei HTTP/Parsing-Fehlern
     */
    public java.util.Map<Integer, String> getNodeAssignmentsForTasks() throws Exception {
        System.out.println("--> getNodeAssignmentsForTasks");
        // 1. HTTP GET auf /v1/admin/cluster/pods
        String url = dns + "/v1/admin/cluster/pods";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> resp = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300)
            throw new RuntimeException("Pods-Request fehlgeschlagen: " + resp.statusCode() + " - " + resp.body());

        // 2. Pods-Response parsen (Liste von Objekten mit "name" und "node")
        JsonNode podsArr = mapper.readTree(resp.body());
        java.util.Map<String, String> podToNode = new java.util.HashMap<>();
        if (podsArr.isArray()) {
            for (JsonNode pod : podsArr) {
                String podName = pod.path("name").asText();
                String nodeName = pod.path("node").asText();
                if (podName != null && !podName.isEmpty())
                    podToNode.put(podName, nodeName);
            }
        }

        // 3. Für jeden Task das Mapping holen (aus registerTasksSteps oder Batch → bodies)
        java.util.Map<Integer, String> result = new java.util.HashMap<>();
        // a) Einzelne Task-Requests
        for (Step s : registerTasksSteps) {
            JsonNode taskBody = s.body;
            if (taskBody == null) continue;
            int id = taskBody.path("id").asInt();
            String runName = taskBody.path("runName").asText();
            String node = podToNode.getOrDefault(runName, null);
            result.put(id, node);
        }
        // b) Batch-Array (falls vorhanden)
        if (registerTasksBatchStep != null && registerTasksBatchStep.body != null && registerTasksBatchStep.body.isArray()) {
            for (JsonNode taskBody : registerTasksBatchStep.body) {
                int id = taskBody.path("id").asInt();
                String runName = taskBody.path("runName").asText();
                String node = podToNode.getOrDefault(runName, null);
                result.put(id, node);
            }
        }


        return result;
    }
    public String killExecution() throws Exception { return doRequest(killExecutionStep); }
    public String resetCluster() throws Exception { return doRequest(resetCluster); }

}