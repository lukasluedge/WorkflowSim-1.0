package org.workflowsim.examples.rnaseqMock;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * trace.csv -> DAX und .datasize (MB)
 * - runtime als Attribut am <job>
 * - datasize-Datei mit "<lfn> <MB>"
 */
public class TraceCsvToDax {

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 4) {
            System.err.println("Aufruf: java TraceCsvToDax <trace.csv> <outDir> [workflowName] [dagHtml]");
            System.exit(2);
        }
        Path csv = Paths.get(args[0]);
        Path outDir = Paths.get(args[1]);
        String wfName = args.length >= 3 ? args[2] : "workflow_from_trace";
        Path dagHtml = (args.length == 4) ? Paths.get(args[3]) : null;

        if (!Files.exists(csv)) throw new IllegalArgumentException("trace.csv nicht gefunden: " + csv);
        Files.createDirectories(outDir);

        // 1) Tasks aus trace.csv lesen (unverändert)
        List<Task> tasks = parseTrace(csv);

        // 2) Optional: Mermaid-Kanten aus HTML extrahieren
        List<ProcEdge> procEdges = Collections.emptyList();
        if (dagHtml != null && Files.exists(dagHtml)) {
            procEdges = parseMermaidEdgesFromHtml(dagHtml);
            if (procEdges.isEmpty()) {
                System.err.println("Hinweis: Keine verwertbaren Mermaid-Kanten im HTML gefunden.");
            }
        }

        // 3) Aufgaben nach Prozess gruppieren (für Instanz-Zuordnung)
        Map<String, List<Task>> byProcess = groupTasksByProcess(tasks);

        // 4) Instanz-Kanten auf Basis der Prozess-Kanten (indexweise Paarung)
        List<TaskEdge> taskEdges = buildTaskEdges(procEdges, byProcess);

        // 5) Ausgaben schreiben
        Path daxPath = outDir.resolve(wfName + ".xml");
        Path datasizePath = outDir.resolve(wfName + ".datasize");

        writeDax(daxPath, wfName, tasks, taskEdges);

        System.out.println("DAX erzeugt:       " + daxPath.toAbsolutePath());
        System.out.println("Datasize erzeugt:  " + datasizePath.toAbsolutePath());
        System.out.println("Tasks: " + tasks.size() + " | Edges: " + taskEdges.size());
    }


    static final class Task {
        public long startMs;
        String id;
        String name;
        String process;
        int cores;
        long memoryBytes;
        double runtimeSec;
        long inputBytes;
        long outputBytes;
    }
    static final class ProcEdge {
        String fromProcess;
        String toProcess;
        ProcEdge(String f, String t) { fromProcess = f; toProcess = t; }
    }
    static final class TaskEdge {
        String parentTaskId;
        String childTaskId;
        TaskEdge(String p, String c) { parentTaskId = p; childTaskId = c; }
    }


    private static List<Task> parseTrace(Path csv) throws IOException {
        List<Task> tasks = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(csv, StandardCharsets.UTF_8)) {
            String header = br.readLine();
            if (header == null) throw new IllegalArgumentException("Leere CSV");
            String[] cols = header.split(",", -1);
            Map<String,Integer> idx = new HashMap<>();
            for (int i=0;i<cols.length;i++) idx.put(cols[i].trim(), i);

            require(idx, "task_id","name","process","cpus","memory",
                    "input_size","read_bytes","write_bytes",
                    "start","complete","duration","realtime");

            String line;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] p = line.split(",", -1);
                Task t = new Task();
                t.id = val(p, idx, "task_id");
                String name = val(p, idx, "name");
                String process = val(p, idx, "process");
                t.name = (name != null && !name.isBlank()) ? name : (process != null ? process : ("task_"+t.id));
                t.process = process;

                t.cores = parseInt(val(p, idx, "cpus"), 1);
                t.memoryBytes = parseLong(val(p, idx, "memory"), 0L);

                long start = parseLong(val(p, idx, "start"), 0L);
                long complete = parseLong(val(p, idx, "complete"), 0L);
                long dur = Math.max(0L, complete - start);
                double rt1 = dur > 0 ? dur / 1000.0 : -1;
                double rt2 = parseLong(val(p, idx, "duration"), -1L) / 1000.0;
                double rt3 = parseLong(val(p, idx, "realtime"), -1L) / 1000.0;
                double runtime = firstPositive(rt1, rt2, rt3, 0.001);
                t.runtimeSec = Math.max(0.001, runtime);

                long inSize = parseLong(val(p, idx, "input_size"), 0L);
                long readBytes = parseLong(val(p, idx, "read_bytes"), 0L);
                long writeBytes = parseLong(val(p, idx, "write_bytes"), 0L);

                t.inputBytes = inSize > 0 ? inSize : readBytes;
                t.outputBytes = writeBytes > 0 ? writeBytes : 8192; // minimaler Dummy-Output

                if (t.inputBytes < 0) t.inputBytes = 0;
                if (t.outputBytes < 0) t.outputBytes = 0;

                tasks.add(t);
            }
        }
        return tasks;
    }

    private static void writeDax(Path out, String workflowName, List<Task> tasks, List<TaskEdge> edges
    ) throws IOException {
        StringBuilder sb = new StringBuilder(128_000);
        sb.append("""
                <?xml version="1.0" encoding="UTF-8"?>
                <adag xmlns="http://pegasus.isi.edu/schema/DAX"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                      xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.3.xsd"
                      version="3.3"
                      name="%s">\n
                """.formatted(escapeXml(workflowName)));

        // Jobs (mit runtime Attribut)
        for (Task t : tasks) {
            sb.append("  <job id=\"").append(escapeXml(t.id))
                    .append("\" name=\"").append(escapeXml(t.name)).append("\"")
                    .append(" runtime=\"").append(String.format(Locale.US, "%.3f", t.runtimeSec)).append("\">\n");

            // Cores/Memory als optionale Profile
            sb.append("    <profile namespace=\"pegasus\" key=\"cores\">")
                    .append(t.cores)
                    .append("</profile>\n");
            long memMB = t.memoryBytes > 0 ? Math.max(1, t.memoryBytes / (1024 * 1024)) : 0;
            if (memMB > 0) {
                sb.append("    <profile namespace=\"pegasus\" key=\"memory\">")
                        .append(memMB)
                        .append("</profile>\n");
            }

            // Uses (WorkflowSim wertet Größe aus datasize-Datei; Uses bleiben zur Struktur)
            String fileName = "null";
            String link = "null";
            long size = 0;
            if (t.inputBytes > 0) {
                fileName = "t" + t.id + "_in.dat";
                link = "input";
                size = t.inputBytes;
                sb.append("    <uses file=\"").append(escapeXml(fileName)).append("\" link=\"").append(link).append("\" transfer=\"true\" register=\"false\" optional=\"false\" size=\"").append(size).append("\"/>\n");
            }
            if (t.outputBytes > 0) {
                fileName = "t" + t.id + "_out.dat";
                link = "output";
                size = t.outputBytes;
                sb.append("    <uses file=\"").append(escapeXml(fileName)).append("\" link=\"").append(link).append("\" transfer=\"true\" register=\"false\" optional=\"false\" size=\"").append(size).append("\"/>\n");

            }


            sb.append("  </job>\n");
        }
        // Kanten (child/parent) – nur wenn aus HTML vorhanden
        if (edges != null && !edges.isEmpty()) {
            Map<String, List<String>> parentsByChild = new LinkedHashMap<>();
            for (TaskEdge e : edges) {
                parentsByChild.computeIfAbsent(e.childTaskId, k -> new ArrayList<>()).add(e.parentTaskId);
            }
            for (Map.Entry<String, List<String>> ent : parentsByChild.entrySet()) {
                sb.append("  <child ref=\"").append(escapeXml(ent.getKey())).append("\">\n");
                for (String parent : ent.getValue()) {
                    sb.append("    <parent ref=\"").append(escapeXml(parent)).append("\"/>\n");
                }
                sb.append("  </child>\n");
            }
        }


        sb.append("</adag>\n");

        try (BufferedWriter bw = Files.newBufferedWriter(out, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            bw.write(sb.toString());
        }
    }

    private static List<ProcEdge> parseMermaidEdgesFromHtml(Path html) throws IOException {
        String content = Files.readString(html, StandardCharsets.UTF_8);

        // 1) Mermaid-Block extrahieren
        int preStart = content.indexOf("<pre");
        if (preStart < 0) return Collections.emptyList();
        int preEnd = content.indexOf("</pre>", preStart);
        if (preEnd < 0) return Collections.emptyList();
        String mermaid = content.substring(preStart, preEnd);

        // 2) vID -> Label (Prozessnamen)
        Map<String,String> nodeToLabel = new HashMap<>();

        // v83([FASTQC])  oder  v37([SAMPLESHEET_CHECK])  oder  v9["literal"]
        Pattern defProc = Pattern.compile("\\b(v\\d+)\\s*\\(\\[([^\\]]+)\\]\\)");
        Pattern defLiteral = Pattern.compile("\\b(v\\d+)\\s*\\[\\\"([^\\\"]+)\\\"\\]");
        Pattern edge = Pattern.compile("\\b(v\\d+)\\s*--?>\\s*(v\\d+)");

        try (Scanner sc = new Scanner(mermaid)) {
            while (sc.hasNextLine()) {
                String line = sc.nextLine().trim();
                if (line.isEmpty()) continue;

                Matcher m1 = defProc.matcher(line);
                while (m1.find()) nodeToLabel.put(m1.group(1), m1.group(2).trim());

                Matcher m2 = defLiteral.matcher(line);
                while (m2.find()) nodeToLabel.put(m2.group(1), m2.group(2).trim());
            }
        }

        // 3) Kanten sammeln und auf Prozessknoten filtern
        List<ProcEdge> procEdges = new ArrayList<>();
        try (Scanner sc = new Scanner(mermaid)) {
            while (sc.hasNextLine()) {
                String line = sc.nextLine().trim();
                if (line.isEmpty()) continue;

                Matcher me = edge.matcher(line);
                while (me.find()) {
                    String from = me.group(1), to = me.group(2);
                    String fromLabel = nodeToLabel.get(from);
                    String toLabel = nodeToLabel.get(to);
                    if (fromLabel != null && toLabel != null) {
                        if (looksLikeProcess(fromLabel) && looksLikeProcess(toLabel)) {
                            procEdges.add(new ProcEdge(normalizeProcess(fromLabel), normalizeProcess(toLabel)));
                        }
                    }
                }
            }
        }

        // 4) Deduplizieren
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        List<ProcEdge> uniq = new ArrayList<>();
        for (ProcEdge e : procEdges) {
            String key = e.fromProcess + "->" + e.toProcess;
            if (seen.add(key)) uniq.add(e);
        }
        return uniq;
    }

    private static boolean looksLikeProcess(String label) {
        // Prozess-Knoten: typischerweise UPPERCASE/UNDERSCORE/Ziffern
        return label.matches("[A-Z0-9_]+");
    }

    private static String normalizeProcess(String proc) {
        if (proc == null) return "";
        // Aus trace.csv: "NFCORE_RNASEQ:RNASEQ:...:FASTQC" -> letzter Abschnitt
        String p = proc.trim();
        int idx = p.lastIndexOf(':');
        String tail = (idx >= 0 && idx+1 < p.length()) ? p.substring(idx+1) : p;
        return tail.replace(' ', '_').toUpperCase(Locale.ROOT);
    }

    // ------------------------- Instanz-Kanten aufbauen -------------------------

    private static Map<String, List<Task>> groupTasksByProcess(List<Task> tasks) {
        Map<String, List<Task>> map = new HashMap<>();
        for (Task t : tasks) {
            String key = normalizeProcess(t.process);
            map.computeIfAbsent(key, k -> new ArrayList<>()).add(t);
        }
        // Stabil sortieren: startMs, dann task_id numerisch
        for (List<Task> list : map.values()) {
            list.sort((a, b) -> {
                int cmp = Long.compare(a.startMs, b.startMs);
                if (cmp != 0) return cmp;
                try { return Integer.compare(Integer.parseInt(a.id), Integer.parseInt(b.id)); }
                catch (Exception e) { return a.id.compareTo(b.id); }
            });
        }
        return map;
    }

    private static List<TaskEdge> buildTaskEdges(List<ProcEdge> procEdges, Map<String, List<Task>> byProcess) {
        if (procEdges == null || procEdges.isEmpty()) return Collections.emptyList();

        List<TaskEdge> result = new ArrayList<>();
        for (ProcEdge pe : procEdges) {
            List<Task> parents = byProcess.getOrDefault(pe.fromProcess, Collections.emptyList());
            List<Task> childs = byProcess.getOrDefault(pe.toProcess, Collections.emptyList());
            if (parents.isEmpty() || childs.isEmpty()) continue;

            int n = Math.min(parents.size(), childs.size());
            for (int i = 0; i < n; i++) {
                Task p = parents.get(i);
                Task c = childs.get(i);
                if (p != null && c != null) result.add(new TaskEdge(p.id, c.id));
            }
        }
        // Dedup
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        List<TaskEdge> uniq = new ArrayList<>();
        for (TaskEdge e : result) {
            String key = e.parentTaskId + "->" + e.childTaskId;
            if (seen.add(key)) uniq.add(e);
        }
        return uniq;
    }

    // Hilfsfunktionen
    private static void require(Map<String,Integer> idx, String... names) {
        for (String n : names) if (!idx.containsKey(n))
            throw new IllegalArgumentException("Spalte fehlt im CSV: " + n);
    }
    private static String val(String[] p, Map<String,Integer> idx, String col) {
        Integer i = idx.get(col);
        if (i == null || i < 0 || i >= p.length) return "";
        return p[i].trim();
    }
    private static int parseInt(String s, int def) {
        try { return Integer.parseInt(s); } catch (Exception e) { return def; }
    }
    private static long parseLong(String s, long def) {
        try { return Long.parseLong(s); } catch (Exception e) { return def; }
    }
    private static double firstPositive(double... vals) {
        for (double v : vals) if (v > 0) return v;
        return 0.0;
    }
    private static String escapeXml(String s) {
        if (s == null) return "";
        return s.replace("&","&amp;").replace("<","&lt;")
                .replace(">","&gt;").replace("\"","&quot;")
                .replace("'","&apos;");
    }
}
