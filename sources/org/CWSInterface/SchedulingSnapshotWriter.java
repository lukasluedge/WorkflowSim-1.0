package org.CWSInterface;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.FileItem;
import org.workflowsim.Job;
import org.workflowsim.WorkflowSimTags;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public final class SchedulingSnapshotWriter {

    private SchedulingSnapshotWriter() {}

    private static long snapshotCounter = 0;

    public static synchronized String nextSnapshotFileName() {
        snapshotCounter++;
        return String.format("scheduling-%06d.json", snapshotCounter);
    }
    public static void writeSchedulingDecisions(String fileName, Map<Integer, String> allocation) {
        try {
            StringBuilder sb = new StringBuilder(128_000);
            double now = org.cloudbus.cloudsim.core.CloudSim.clock();
            //create a text file that stores the contents of the Map<integer, String> allocation
            for (Map.Entry<Integer, String> entry : allocation.entrySet()) {
                sb.append(entry.getKey()).append(" ").append(entry.getValue()).append("\n");
            }
            Files.writeString(Path.of(fileName), sb.toString(), StandardCharsets.UTF_8);
        } catch (Exception e) {}
    }

    public static void writeFullSnapshot(String fileName, List<?> readyCloudlets, List<?> vms, List<?> scheduled) {
        try {
            StringBuilder sb = new StringBuilder(128_000);
            double now = org.cloudbus.cloudsim.core.CloudSim.clock();

            // IDs von Ready/Scheduled für Markierungen
            Set<Integer> readyIds = new HashSet<>();
            if (readyCloudlets != null) {
                for (Object o : readyCloudlets) {
                    Cloudlet c = (Cloudlet) o;
                    readyIds.add(c.getCloudletId());
                }
            }
            Set<Integer> scheduledIds = new HashSet<>();
            if (scheduled != null) {
                for (Object o : scheduled) {
                    Cloudlet c = (Cloudlet) o;
                    scheduledIds.add(c.getCloudletId());
                }
            }

            // Startmenge an bekannten Jobs
            Map<Integer, Job> jobsById = new LinkedHashMap<>();
            if (readyCloudlets != null) {
                for (Object o : readyCloudlets) {
                    if (o instanceof Job j) {
                        jobsById.put(j.getCloudletId(), j);
                    }
                }
            }
            if (scheduled != null) {
                for (Object o : scheduled) {
                    if (o instanceof Job j) {
                        jobsById.put(j.getCloudletId(), j);
                    }
                }
            }

            // Gesamten DAG transitiv einsammeln (Eltern + Kinder)
            Queue<Job> q = new ArrayDeque<>(jobsById.values());
            Set<Integer> enqueued = new HashSet<>(jobsById.keySet());
            while (!q.isEmpty()) {
                Job cur = q.poll();

                // Kinder
                try {
                    List<?> children = cur.getChildList();
                    if (children != null) {
                        for (Object ch : children) {
                            if (!(ch instanceof Job cj)) continue;
                            int id = cj.getCloudletId();
                            if (jobsById.putIfAbsent(id, cj) == null && enqueued.add(id)) {
                                q.add(cj);
                            }
                        }
                    }
                } catch (Throwable ignored) {}

                // Eltern
                try {
                    List<?> parents = cur.getParentList();
                    if (parents != null) {
                        for (Object p : parents) {
                            if (!(p instanceof Job pj)) continue;
                            int id = pj.getCloudletId();
                            if (jobsById.putIfAbsent(id, pj) == null && enqueued.add(id)) {
                                q.add(pj);
                            }
                        }
                    }
                } catch (Throwable ignored) {}
            }

            // Kanten Parent->Child deduplizieren
            List<String> edges = new ArrayList<>();
            Set<String> edgeSet = new HashSet<>();
            for (Job j : jobsById.values()) {
                try {
                    List<?> children = j.getChildList();
                    if (children != null) {
                        for (Object ch : children) {
                            if (!(ch instanceof Job cj)) continue;
                            String key = j.getCloudletId() + "->" + cj.getCloudletId();
                            if (edgeSet.add(key)) {
                                edges.add("{ \"from\": " + j.getCloudletId() + ", \"to\": " + cj.getCloudletId() + " }");
                            }
                        }
                    }
                } catch (Throwable ignored) {}
            }

            // JSON erzeugen
            sb.append("{\n");
            sb.append("  \"meta\": {\n");
            sb.append("    \"simulationTime\": ").append(formatDouble(now)).append(",\n");
            sb.append("    \"minTimeBetweenEvents\": ").append(formatDouble(org.cloudbus.cloudsim.core.CloudSim.getMinTimeBetweenEvents())).append(",\n");
            sb.append("    \"readyCloudletsCount\": ").append(readyCloudlets == null ? 0 : readyCloudlets.size()).append(",\n");
            sb.append("    \"scheduledCount\": ").append(scheduled == null ? 0 : scheduled.size()).append(",\n");
            sb.append("    \"vmCount\": ").append(vms == null ? 0 : vms.size()).append("\n");
            sb.append("  },\n");

            // Ready-Cloudlets
            sb.append("  \"readyCloudlets\": [\n");
            if (readyCloudlets != null) {
                for (int i = 0; i < readyCloudlets.size(); i++) {
                    Cloudlet cl = (Cloudlet) readyCloudlets.get(i);
                    boolean isJob = cl instanceof Job;
                    Job job = isJob ? (Job) cl : null;

                    double cpuUtil = cl.getUtilizationOfCpu(now);
                    double ramUtil = cl.getUtilizationOfRam(now);
                    double bwUtil  = cl.getUtilizationOfBw(now);

                    sb.append("    {\n");
                    sb.append("      \"cloudletId\": ").append(cl.getCloudletId()).append(",\n");
                    sb.append("      \"length\": ").append(cl.getCloudletLength()).append(",\n");
                    sb.append("      \"pes\": ").append(cl.getNumberOfPes()).append(",\n");
                    sb.append("      \"status\": ").append(cl.getCloudletStatus()).append(",\n");
                    sb.append("      \"vmId\": ").append(cl.getVmId()).append(",\n");
                    sb.append("      \"fileSize\": ").append(cl.getCloudletFileSize()).append(",\n");
                    sb.append("      \"outputSize\": ").append(cl.getCloudletOutputSize()).append(",\n");
                    sb.append("      \"submissionTime\": ").append(formatDouble(cl.getSubmissionTime())).append(",\n");
                    sb.append("      \"execStartTime\": ").append(formatDouble(cl.getExecStartTime())).append(",\n");
                    sb.append("      \"finishTime\": ").append(formatDouble(cl.getFinishTime())).append(",\n");
                    sb.append("      \"cpuUtil\": ").append(formatDouble(cpuUtil)).append(",\n");
                    sb.append("      \"ramUtil\": ").append(formatDouble(ramUtil)).append(",\n");
                    sb.append("      \"bwUtil\": ").append(formatDouble(bwUtil)).append(",\n");
                    sb.append("      \"utilModelCpu\": ").append(jsonString(cl.getUtilizationModelCpu() == null ? null : cl.getUtilizationModelCpu().getClass().getSimpleName())).append(",\n");
                    sb.append("      \"utilModelRam\": ").append(jsonString(cl.getUtilizationModelRam() == null ? null : cl.getUtilizationModelRam().getClass().getSimpleName())).append(",\n");
                    sb.append("      \"utilModelBw\": ").append(jsonString(cl.getUtilizationModelBw() == null ? null : cl.getUtilizationModelBw().getClass().getSimpleName())).append(",\n");

                    List<String> req = cl.getRequiredFiles();
                    sb.append("      \"requiredFiles\": [");
                    if (req != null && !req.isEmpty()) {
                        sb.append("\n");
                        for (int r = 0; r < req.size(); r++) {
                            sb.append("        ").append(jsonString(req.get(r)));
                            if (r < req.size() - 1) sb.append(",");
                            sb.append("\n");
                        }
                        sb.append("      ],\n");
                    } else {
                        sb.append("],\n");
                    }

                    sb.append("      \"isJob\": ").append(isJob).append(",\n");
                    if (isJob) {
                        sb.append("      \"job\": {\n");
                        sb.append("        \"depth\": ").append(job.getDepth()).append(",\n");
                        sb.append("        \"classType\": ").append(job.getClassType()).append(",\n");
                        List<FileItem> files = job.getFileList();
                        sb.append("        \"files\": [\n");
                        for (int f = 0; f < files.size(); f++) {
                            FileItem fi = files.get(f);
                            sb.append("          {\n");
                            sb.append("            \"name\": ").append(jsonString(fi.getName())).append(",\n");
                            sb.append("            \"size\": ").append(fi.getSize()).append(",\n");
                            sb.append("            \"isRealInput\": ").append(fi.isRealInputFile(files)).append("\n");
                            sb.append("          }");
                            if (f < files.size() - 1) sb.append(",");
                            sb.append("\n");
                        }
                        sb.append("        ]\n");
                        sb.append("      }\n");
                    }
                    sb.append("    }");
                    if (i < readyCloudlets.size() - 1) sb.append(",");
                    sb.append("\n");
                }
            }
            sb.append("  ],\n");

            // Scheduled-Liste
            sb.append("  \"scheduledCloudlets\": [\n");
            if (scheduled != null) {
                for (int i = 0; i < scheduled.size(); i++) {
                    Cloudlet scl = (Cloudlet) scheduled.get(i);
                    sb.append("    { \"cloudletId\": ").append(scl.getCloudletId())
                            .append(", \"vmId\": ").append(scl.getVmId()).append(" }");
                    if (i < scheduled.size() - 1) sb.append(",");
                    sb.append("\n");
                }
            }
            sb.append("  ],\n");

            // VMs
            sb.append("  \"vms\": [\n");
            if (vms != null) {
                for (int i = 0; i < vms.size(); i++) {
                    CondorVM vm = (CondorVM) vms.get(i);
                    org.cloudbus.cloudsim.Vm baseVm = vm;
                    double totalCpuUtil = baseVm.getTotalUtilizationOfCpu(now);
                    double totalCpuUtilMips = baseVm.getTotalUtilizationOfCpuMips(now);

                    sb.append("    {\n");
                    sb.append("      \"id\": ").append(baseVm.getId()).append(",\n");
                    sb.append("      \"uid\": ").append(jsonString(baseVm.getUid())).append(",\n");
                    sb.append("      \"userId\": ").append(baseVm.getUserId()).append(",\n");
                    sb.append("      \"state\": ").append(vm.getState()).append(",\n");
                    sb.append("      \"mips\": ").append(baseVm.getMips()).append(",\n");
                    sb.append("      \"numberOfPes\": ").append(baseVm.getNumberOfPes()).append(",\n");
                    sb.append("      \"ram\": ").append(baseVm.getRam()).append(",\n");
                    sb.append("      \"bw\": ").append(baseVm.getBw()).append(",\n");
                    sb.append("      \"size\": ").append(baseVm.getSize()).append(",\n");
                    sb.append("      \"vmm\": ").append(jsonString(baseVm.getVmm())).append(",\n");
                    sb.append("      \"inMigration\": ").append(baseVm.isInMigration()).append(",\n");
                    sb.append("      \"beingInstantiated\": ").append(baseVm.isBeingInstantiated()).append(",\n");
                    sb.append("      \"host\": ");
                    if (baseVm.getHost() != null) {
                        sb.append("{ \"id\": ").append(baseVm.getHost().getId()).append(" }");
                    } else {
                        sb.append("null");
                    }
                    sb.append(",\n");
                    sb.append("      \"currentAllocatedRam\": ").append(baseVm.getCurrentAllocatedRam()).append(",\n");
                    sb.append("      \"currentAllocatedBw\": ").append(baseVm.getCurrentAllocatedBw()).append(",\n");

                    List<Double> allocMips = baseVm.getCurrentAllocatedMips();
                    sb.append("      \"currentAllocatedMips\": ");
                    if (allocMips != null) {
                        sb.append("[");
                        for (int m = 0; m < allocMips.size(); m++) {
                            sb.append(formatDouble(allocMips.get(m)));
                            if (m < allocMips.size() - 1) sb.append(", ");
                        }
                        sb.append("]");
                    } else {
                        sb.append("null");
                    }
                    sb.append(",\n");

                    List<Double> reqMips = baseVm.getCurrentRequestedMips();
                    sb.append("      \"requestedMips\": [");
                    for (int r = 0; r < reqMips.size(); r++) {
                        sb.append(formatDouble(reqMips.get(r)));
                        if (r < reqMips.size() - 1) sb.append(", ");
                    }
                    sb.append("],\n");
                    sb.append("      \"requestedTotalMips\": ").append(formatDouble(baseVm.getCurrentRequestedTotalMips())).append(",\n");
                    sb.append("      \"requestedMaxMips\": ").append(formatDouble(baseVm.getCurrentRequestedMaxMips())).append(",\n");
                    sb.append("      \"requestedRam\": ").append(baseVm.getCurrentRequestedRam()).append(",\n");
                    sb.append("      \"requestedBw\": ").append(baseVm.getCurrentRequestedBw()).append(",\n");
                    sb.append("      \"totalCpuUtil\": ").append(formatDouble(totalCpuUtil)).append(",\n");
                    sb.append("      \"totalCpuUtilMips\": ").append(formatDouble(totalCpuUtilMips)).append(",\n");
                    sb.append("      \"cloudletScheduler\": ").append(jsonString(baseVm.getCloudletScheduler() == null ? null : baseVm.getCloudletScheduler().getClass().getSimpleName())).append("\n");

                    sb.append("    }");
                    if (i < vms.size() - 1) sb.append(",");
                    sb.append("\n");
                }
            }
            sb.append("  ],\n");

            // DAG (gesamter Graph)
            sb.append("  \"dag\": {\n");
            sb.append("    \"nodes\": [\n");
            int idx = 0;
            for (Job j : jobsById.values()) {
                if (j == null) continue;
                boolean inReady = readyIds.contains(j.getCloudletId());
                boolean inSched = scheduledIds.contains(j.getCloudletId());
                sb.append("      {\n");
                sb.append("        \"cloudletId\": ").append(j.getCloudletId()).append(",\n");
                sb.append("        \"status\": ").append(j.getCloudletStatus()).append(",\n");
                sb.append("        \"vmId\": ").append(j.getVmId()).append(",\n");
                sb.append("        \"depth\": ").append(j.getDepth()).append(",\n");
                sb.append("        \"classType\": ").append(j.getClassType()).append(",\n");
                sb.append("        \"inReadyQueue\": ").append(inReady).append(",\n");
                sb.append("        \"inScheduledList\": ").append(inSched).append("\n");
                sb.append("      }");
                if (idx++ < jobsById.size() - 1) sb.append(",");
                sb.append("\n");
            }
            sb.append("    ],\n");
            sb.append("    \"edges\": [\n");
            for (int i = 0; i < edges.size(); i++) {
                sb.append("      ").append(edges.get(i));
                if (i < edges.size() - 1) sb.append(",");
                sb.append("\n");
            }
            sb.append("    ]\n");
            sb.append("  }\n");

            sb.append("}\n");

            Files.writeString(Path.of(fileName), sb.toString(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            Log.printLine("[Scheduling] Fehler beim Schreiben des Voll-Snapshots: " + e.getMessage());
        }
    }

    private static String jsonString(String s) {
        if (s == null) return "null";
        String esc = s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
        return "\"" + esc + "\"";
    }

    private static String formatDouble(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return "null";
        return String.format(java.util.Locale.ROOT, "%.6f", v);
    }

    public static final class ExternalSchedulerConfig {
        public String baseUrl = "http://localhost:8080";
        public String execution = "my-exec";
        public String strategy = "fifo-rr";
        public boolean locationAware = false;
        public String namespace = "default";
        public String workDir = "/data/workdir";
        public String dns = "http://localhost:8080";
        public String copyStrategy = "daemon"; // optional
        public String memoryPredictor = "";    // optional, meist leer
        public String localWorkDir = "/tmp";   // optional
        private boolean initScheduler = true;
        // Optional: IDs von Tasks, die seit dem letzten Scheduler-Aufruf abgeschlossen wurden
        public java.util.List<Integer> completedSinceLast = java.util.Collections.emptyList();

        public ExternalSchedulerConfig() {
        }

        public ExternalSchedulerConfig withBaseUrl(String v) { this.baseUrl = v; return this; }
        public ExternalSchedulerConfig withExecution(String v) { this.execution = v; return this; }
        public ExternalSchedulerConfig withStrategy(String v) { this.strategy = v; return this; }
        public ExternalSchedulerConfig withLocationAware(boolean v) { this.locationAware = v; return this; }
        public ExternalSchedulerConfig withNamespace(String v) { this.namespace = v; return this; }
        public ExternalSchedulerConfig withWorkDir(String v) { this.workDir = v; return this; }
        public ExternalSchedulerConfig withDns(String v) { this.dns = v; return this; }
        public ExternalSchedulerConfig withCopyStrategy(String v) { this.copyStrategy = v; return this; }
        public ExternalSchedulerConfig withMemoryPredictor(String v) { this.memoryPredictor = v; return this; }
        public ExternalSchedulerConfig withLocalWorkDir(String v) { this.localWorkDir = v; return this; }

    }

    /**
     * Erzeugt ein JSON mit allen Schritten und den zugehörigen HTTP-Methoden/URLs/Bodys,
     * sodass dein externes Python-Skript diese Befehle 1:1 ausführen könnte.
     *
     * Schritte:
     * - registerScheduler (POST)
     * - createNodes (POST je VM)
     * - submitDAG (POST Vertices, POST Edges)
     * - startBatch (PUT)
     * - registerTasks (POST je Cloudlet aus readyCloudlets)
     * - endBatch (PUT)
     */
    public static void writeExternalSchedulerSteps(
            String fileName,
            List<?> vms,
            List<?> readyCloudlets,
            List<?> scheduled,
            List<Cloudlet> finishedCloudlets,
            ExternalSchedulerConfig cfg
    ) {
        try {
            double now = org.cloudbus.cloudsim.core.CloudSim.clock();

            // 1) DAG vollständig ermitteln (transitive Hülle)
            Map<Integer, Job> jobsById = new LinkedHashMap<>();
            if (readyCloudlets != null) {
                for (Object o : readyCloudlets) {
                    if (o instanceof Job j) {
                        jobsById.put(j.getCloudletId(), j);
                    }
                }
            }
            if (scheduled != null) {
                for (Object o : scheduled) {
                    if (o instanceof Job j) {
                        jobsById.put(j.getCloudletId(), j);
                    }
                }
            }
            Queue<Job> q = new ArrayDeque<>(jobsById.values());
            Set<Integer> seen = new HashSet<>(jobsById.keySet());
            while (!q.isEmpty()) {
                Job cur = q.poll();
                try {
                    List<?> children = cur.getChildList();
                    if (children != null) {
                        for (Object ch : children) {
                            if (ch instanceof Job cj && seen.add(cj.getCloudletId())) {
                                jobsById.put(cj.getCloudletId(), cj);
                                q.add(cj);
                            }
                        }
                    }
                } catch (Throwable ignored) {}
                try {
                    List<?> parents = cur.getParentList();
                    if (parents != null) {
                        for (Object p : parents) {
                            if (p instanceof Job pj && seen.add(pj.getCloudletId())) {
                                jobsById.put(pj.getCloudletId(), pj);
                                q.add(pj);
                            }
                        }
                    }
                } catch (Throwable ignored) {}
            }

            // Vertices/Edges im gewünschten schlanken Format
            List<String> verticesJson = new ArrayList<>();
            for (Job j : jobsById.values()) {
                int uid = j.getCloudletId();
                String label = "Job_" + uid;
                verticesJson.add("{\"uid\":" + uid + ",\"label\":" + jsonString(label) + ",\"type\":\"PROCESS\"}");
            }
            List<String> edgesJson = new ArrayList<>();
            Set<String> edgeSet = new HashSet<>();
            int edgeUid = 1;
            for (Job j : jobsById.values()) {
                try {
                    List<?> children = j.getChildList();
                    if (children != null) {
                        for (Object ch : children) {
                            if (!(ch instanceof Job cj)) continue;
                            String key = j.getCloudletId() + "->" + cj.getCloudletId();
                            if (edgeSet.add(key)) {
                                edgesJson.add("{\"uid\":" + (edgeUid++) + ",\"from\":" + j.getCloudletId() + ",\"to\":" + cj.getCloudletId() + "}");
                            }
                        }
                    }
                } catch (Throwable ignored) {}
            }

            // 2) Nodes aus VMs erzeugen
            List<String> nodesJson = new ArrayList<>();
            List<String> nodeBodiesJson = new ArrayList<>();
            double avgRamMiB = 0.0;
            int vmCount = 0;
            if (vms != null) {
                for (Object o : vms) {
                    if (!(o instanceof CondorVM vm)) continue;
                    vmCount++;
                    avgRamMiB += (double) vm.getRam() /vm.getNumberOfPes();
                    String name = "vm-" + vm.getId();
                    // RAM in Gi abrunden
                    int ramMB = vm.getRam();
                    int allocRam = vm.getCurrentAllocatedRam();
                    int effectiveRam = ramMB-allocRam;
                    String memStr = allocRam + "Mi";
                    String cpuStr = String.valueOf(vm.getNumberOfPes());
                    if (vm.getState() != WorkflowSimTags.VM_STATUS_BUSY) {
                        nodesJson.add("{\"method\":\"POST\",\"url\":" + jsonString(cfg.baseUrl + "/v1/admin/cluster/node")
                                + ",\"body\":{\"name\":" + jsonString(name) + ",\"memory\":" + jsonString(memStr) + ",\"cpu\":" + jsonString(cpuStr) + "}}");
                    }
                }
            }
            if (vmCount > 0) avgRamMiB /= vmCount;
            long defaultTaskMemBytes = (long) Math.max(1, Math.round(avgRamMiB/2)) * 1024L * 1024L;

            // 3) Tasks aus readyCloudlets ableiten (jetzt als ein gemeinsamer POST auf /v1/scheduler/{execution}/tasks)
            List<String> tasksJson = new ArrayList<>(); // kept for structure, but we will use a single batch step below
            List<String> taskBodiesJson = new ArrayList<>();
            int tasksInBatch = 0;
            if (readyCloudlets != null) {
                for (Object o : readyCloudlets) {
                    if (!(o instanceof Cloudlet cl)) continue;
                    int id = cl.getCloudletId();
                    String taskName = "Job_" + id;
                    String runName = "cl_" + id;
                    double cpus = Math.max(1, cl.getNumberOfPes());
                    long memBytes = (cl.getMemoryRequirementMB() != -1) ? (cl.getMemoryRequirementMB() * 1024L * 1024L) : defaultTaskMemBytes;
                    String body = "{" +
                            "\"id\":" + id + "," +
                            "\"task\":" + jsonString(taskName) + "," +
                            "\"runName\":" + jsonString(runName) + "," +
                            "\"name\":" + jsonString(taskName) + "," +
                            "\"workDir\":" + jsonString(cfg.workDir) + "," +
                            "\"cpus\":" + String.format(java.util.Locale.ROOT, "%.1f", cpus) + "," +
                            "\"memoryInBytes\":" + memBytes + "," +
                            "\"repetition\":0" +
                            "}";
                    taskBodiesJson.add(body);
                    tasksInBatch++;
                }
            }

            //register the output files for the completed tasks
            List<String> fileBodies = new ArrayList<>();
            List<String> fileUrls = new ArrayList<>();
            if (finishedCloudlets != null) {
                for (Object o : finishedCloudlets) {
                    if (!(o instanceof Cloudlet cl)) continue;
                    int VMid = cl.getVmId();
                    int id = cl.getCloudletId();
                    String VMName = "vm-" + VMid;
                    String runName = "cl_" + id;
                    String path = "/sim/" + cfg.execution + "/" + runName + "/out/t_" + id + ".dat";
                    String body = "{" +
                            "\"path\": \"" + path + "\"," +
                            "\"size\": " + cl.getCloudletOutputSize() + "," +
                            "\"timestamp\": " + System.currentTimeMillis() + "," +
                            "\"locationWrapperID\": " + -1 +
                            "}";
                    String url = cfg.baseUrl + "/v1/file/" + cfg.execution + "/location/add/" + VMName;
                    fileBodies.add(body);
                    fileUrls.add(url);
                }
            }

            // 4) registerScheduler Body
            String regUrl = cfg.baseUrl + "/v1/scheduler/" + cfg.execution;
            String regBody = "{"
                    + "\"namespace\":" + jsonString(cfg.namespace) + ","
                    + "\"strategy\":" + jsonString(cfg.strategy) + ","
                    + "\"dns\":" + jsonString(cfg.dns) + ","
                    + "\"locationAware\":" + cfg.locationAware + ","
                    + "\"traceEnabled\":true,"
                    + "\"localWorkDir\":" + jsonString(cfg.localWorkDir) + ","
                    + "\"workDir\":" + jsonString(cfg.workDir) + ","
                    + "\"copyStrategy\":" + jsonString(cfg.copyStrategy) + ","
                    + "\"memoryPredictor\":" + jsonString(cfg.memoryPredictor)
                    + "}";

            // 5) zusammenbauen
            StringBuilder sb = new StringBuilder(64_000);
            sb.append("{\n");
            sb.append("  \"meta\": { \"simulationTime\": ").append(formatDouble(now)).append(" },\n");
            sb.append("  \"steps\": {\n");
            if (cfg.initScheduler) {
                sb.append("    \"registerScheduler\": {\"method\":\"POST\",\"url\": ").append(jsonString(regUrl)).append(",\"body\": ").append(regBody).append("},\n");
                cfg.initScheduler = false;
            }


            sb.append("    \"createNodes\": [\n");
            for (int i = 0; i < nodesJson.size(); i++) {
                sb.append("      ").append(nodesJson.get(i));
                if (i < nodesJson.size() - 1) sb.append(",");
                sb.append("\n");
            }
            sb.append("    ],\n");

            // DAG submit (Vertices/Edges)
            String dagVerticesUrl = cfg.baseUrl + "/v1/scheduler/" + cfg.execution + "/DAG/vertices";
            String dagEdgesUrl = cfg.baseUrl + "/v1/scheduler/" + cfg.execution + "/DAG/edges";
            sb.append("    \"submitDAG\": {\n");
            sb.append("      \"vertices\": {\"method\":\"POST\",\"url\": ").append(jsonString(dagVerticesUrl)).append(",\"body\": [\n");
            for (int i = 0; i < verticesJson.size(); i++) {
                sb.append("        ").append(verticesJson.get(i));
                if (i < verticesJson.size() - 1) sb.append(",");
                sb.append("\n");
            }
            sb.append("      ]},\n");
            sb.append("      \"edges\": {\"method\":\"POST\",\"url\": ").append(jsonString(dagEdgesUrl)).append(",\"body\": [\n");
            for (int i = 0; i < edgesJson.size(); i++) {
                sb.append("        ").append(edgesJson.get(i));
                if (i < edgesJson.size() - 1) sb.append(",");
                sb.append("\n");
            }
            sb.append("      ]}\n");
            sb.append("    },\n");

            // Batch Start
            String startBatchUrl = cfg.baseUrl + "/v1/scheduler/" + cfg.execution + "/startBatch";
            sb.append("    \"startBatch\": {\"method\":\"PUT\",\"url\": ").append(jsonString(startBatchUrl)).append("},\n");

            // Tasks registrieren: EIN Request mit POST auf /v1/scheduler/{execution}/tasks und Body als Array von Task-Objekten
            String registerTasksUrl = cfg.baseUrl + "/v1/scheduler/" + cfg.execution + "/tasks";
            sb.append("    \"registerTasks\": {\"method\":\"POST\",\"url\": ").append(jsonString(registerTasksUrl)).append(",\"body\": [\n");
            for (int i = 0; i < taskBodiesJson.size(); i++) {
                sb.append("      ").append(taskBodiesJson.get(i));
                if (i < taskBodiesJson.size() - 1) sb.append(",");
                sb.append("\n");
            }
            sb.append("    ]},\n");

            // Batch Ende
            String endBatchUrl = cfg.baseUrl + "/v1/scheduler/" + cfg.execution + "/endBatch";
            sb.append("    \"endBatch\": {\"method\":\"PUT\",\"url\": ").append(jsonString(endBatchUrl)).append(",\"body\": ").append(tasksInBatch).append("},\n");

            //register output files of completed tasks
            if (!fileBodies.isEmpty() && !fileUrls.isEmpty()) {
                sb.append("    \"registerOutputFiles\": [\n");
                for (int i = 0; i < fileBodies.size(); i++) {
                    sb.append("      {\"method\":\"POST\",\"url\": ").append(jsonString(fileUrls.get(i))).append(",\"body\": ").append(fileBodies.get(i)).append("}");
                    if (i < fileBodies.size() - 1) sb.append(",");
                    sb.append("\n");
                }
                sb.append("    ],\n");
            }

            //delete finished tasks
            List<String> podNames = finishedCloudlets.stream().map(n->"\"cl_" + n.getCloudletId()+"\"").toList();
            sb.append("    \"reportCompletedTasks\": {\"method\":\"POST\", \"url\": ").append(jsonString(cfg.baseUrl + "/v1/admin/cluster/pods/" + cfg.namespace)).append(", \"body\": ").append(podNames).append("},\n");

            //kill CWS scheduler execution
            String killExec = cfg.baseUrl + "/v1/scheduler/" + cfg.execution;
            sb.append("    \"killExecution\": {\"method\":\"DELETE\",\"url\": ").append(jsonString(killExec)).append("},\n");

            //reset Cluster
            String resetCluster = cfg.baseUrl + "/v1/admin/cluster/reset";
            sb.append("    \"resetCluster\": {\"method\":\"DELETE\",\"url\": ").append(jsonString(resetCluster)).append("}\n");

            sb.append("  }\n");
            sb.append("}\n");

            Files.writeString(Path.of(fileName), sb.toString(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            Log.printLine("[Scheduling] Fehler beim Schreiben des External-Steps-Snapshots: " + e.getMessage());
        }
    }

}
