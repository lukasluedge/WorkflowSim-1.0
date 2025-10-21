package org.CWSInterface;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.AbstractMap;

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
    private Step requestedCopiesStep;
    private Step reportCompletedTasksStep;
    private static String dns;

    // Konstruktor: lädt und parst die JSON-Datei
    public HttpRunner(String jsonPath, boolean schedulerRegistered) throws Exception {
        JsonNode root = mapper.readTree(new File(jsonPath));
        JsonNode steps = root.path("steps");

        if (!schedulerRegistered) {
            registerSchedulerStep = parseStep(steps.path("registerScheduler"));
            dns = registerSchedulerStep.body.path("dns").asText();
            for (JsonNode nodeStep : steps.path("createNodes")) {
                createNodesSteps.add(parseStep(nodeStep));
            }
            dagVerticesStep = parseStep(steps.path("submitDAG").path("vertices"));
            dagEdgesStep = parseStep(steps.path("submitDAG").path("edges"));
        }



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
        requestedCopiesStep = parseStep(steps.path("getCopyRequests"));
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
     * Holt das Mapping TaskID -> NodeName direkt vom Scheduler (oder Pods-Fallback) und gibt es als Map zurück.
     * Entspricht der geforderten Schnittstelle mit optionalem Await.
     */
    public Map<Integer, String> getTaskToNodeMappingFromScheduler(String execution) throws Exception {
        String url = dns + "/v1/admin/cluster/mapping/" + execution;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> resp = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new RuntimeException("Scheduler mapping request failed: " + resp.statusCode() + " - " + resp.body());
        }
        String body = resp.body();
        if (body == null || body.isEmpty()) return new HashMap<>();

        JsonNode root = mapper.readTree(body);
        JsonNode mappingNode = root != null && root.isObject() && root.has("mapping") ? root.get("mapping") : root;
        Map<Integer,String> out = new HashMap<>();
        if (mappingNode != null && mappingNode.isObject()) {
            mappingNode.fields().forEachRemaining(e -> {
                try {
                    Integer id = Integer.valueOf(e.getKey().substring(3));
                    String v = e.getValue().asText(null);
                    if (v != null) out.put(id, v);
                } catch (NumberFormatException ignored) {}
            });
        }
        return out;
    }

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

    /**
     * Holt die geplanten Datei-Kopien vom Scheduler und gibt sie als Map zurück.
     * Entspricht der neuen drainPlannedCopies()-Schnittstelle (filename -> targetNode).
     */
    public Map<String, String> drainPlannedCopies() throws Exception {
        if (requestedCopiesStep == null) throw new IllegalStateException("getCopyRequests step not configured");
        String body = doRequest(requestedCopiesStep);
        return parseRequestedCopiesToMap(body);
    }

    /**
     * Abwärtskompatibel: Delegiert auf drainPlannedCopies().
     */
    public Map<String, String> getRequestedCopiesAsMap() throws Exception { return drainPlannedCopies(); }

    /**
     * Optional: als Liste von Map.Entry<String,String>.
     */
    public List<Map.Entry<String,String>> getRequestedCopiesAsList() throws Exception {
        Map<String,String> map = drainPlannedCopies();
        List<Map.Entry<String,String>> out = new ArrayList<>();
        for (Map.Entry<String,String> e : map.entrySet()) out.add(new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue()));
        return out;
    }

    // -------- intern: Parser --------
    private Map<String,String> parseRequestedCopiesToMap(String json) throws Exception {
        Map<String,String> result = new HashMap<>();
        if (json == null || json.isEmpty()) return result;
        JsonNode root = mapper.readTree(json);
        if (root == null || root.isNull()) return result;
        if (root.isObject()) { root.fields().forEachRemaining(e -> { String f = e.getKey(); String n = e.getValue().asText(null); if (f != null && n != null) result.put(f, n); }); return result; }
        if (root.isArray()) for (JsonNode item : root) {
            if (item == null || item.isNull()) continue;
            if (item.isObject()) {
                String f = item.hasNonNull("filename") ? item.get("filename").asText() : item.hasNonNull("file") ? item.get("file").asText() : item.hasNonNull("key") ? item.get("key").asText() : null;
                String n = item.hasNonNull("targetNode") ? item.get("targetNode").asText() : item.hasNonNull("node") ? item.get("node").asText() : item.hasNonNull("value") ? item.get("value").asText() : null;
                if (f != null && n != null) result.put(f, n);
            } else if (item.isArray() && item.size() >= 2) {
                String f = item.get(0).asText(null); String n = item.get(1).asText(null); if (f != null && n != null) result.put(f, n);
            }
        }
        return result;
    }
}