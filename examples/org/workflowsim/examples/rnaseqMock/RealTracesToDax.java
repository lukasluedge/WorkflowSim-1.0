package org.workflowsim.examples.rnaseqMock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.print.attribute.standard.JobName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * DagToAdag
 * Usage:
 *   java DagToAdag physicalDag.json [filename-mapping.csv] output.xml
 *
 * filename-mapping.csv (optional) should be CSV with header or no header:
 * original,newName
 * e.g.:
 * /path/to/sample_R1.fastq.gz,sample_R1.fastq.gz
 */
public class RealTracesToDax {

    private static Map<String, Long> fileSizesMap = new HashMap<>();
    private static Map<String, Set<String>> parentsOf = new LinkedHashMap<>();
    private static Map<String, List<String>> nameToTaskNameInput = new HashMap<>();
    private static Map<String, List<String>> nameToTaskNameOutput = new HashMap<>();
    private static Set<String> fileNamesIn = new HashSet<>();
    private static Set<String> fileNamesOut = new HashSet<>();
    private static Map<String, List<UseFile>> fileMap = new HashMap<>();
    private static Map<String, Job> jobs = new HashMap<>();
    private static Map<String, Job> jobsByName = new HashMap<>();

    // Simple data structures
    static class Job {
        String id;
        String namespace = "default";
        String taskName;
        long runtime = 100l; // optional
        Integer cores = null;  // optional
        long memory = 100l;
        List<UseFile> uses = new ArrayList<>();
        private int getSortId(){
            if (id.startsWith("stageIn")) return -1;
            int ID = Integer.parseInt(id.substring(1));
            return ID;
        }
    }

    static class UseFile {
        String path;    //tag: "file
        String link;    // input|output
        long size;
        @Override
        public String toString() {
            return "UseFile{" +
                    "path='" + path + '\'' +
                    ", link='" + link + '\'' +
                    ", size=" + size +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        String dagJsonPath = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\rnaseqMock\\orig_traces\\orig\\physicalDag.json";
        String inputCsvPath = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\rnaseqMock\\orig_traces\\orig\\input.csv";
        String outputCsvPath = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\rnaseqMock\\orig_traces\\orig\\output.csv";
        String traceCsvPath = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\rnaseqMock\\orig_traces\\orig\\trace.csv";
        String outPath = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\rnaseqMock\\orig_traces\\rnaseq_real.xml";




        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(Path.of(dagJsonPath).toFile());
        JsonNode dagRoot = root.path("edges");

        for (JsonNode edge : dagRoot) {
            JsonNode from = edge.get("from");
            JsonNode to = edge.get("to");

            // Helper: get node id (ID)
            String fromId = nodeIdFor(from);
            String toId = nodeIdFor(to);

            // Create job entries for 'from' and 'to' if they are processes
            if (booleanTrue(from, "process")) {
                if (jobs.get(fromId) == null) {
                    Job jj = new Job();
                    jj.id = fromId;
                    jj.taskName = from.get("taskName").asText();
                    jobs.put(fromId, jj);
                    jobsByName.put(jj.taskName, jj);
                }
            }
            if (booleanTrue(to, "process")) {
                if (jobs.get(toId) == null) {
                    Job jj = new Job();
                    jj.id = toId;
                    jj.taskName = to.get("taskName").asText();
                    jobs.put(toId, jj);
                    jobsByName.put(jj.taskName, jj);
                }
            }



            if (booleanTrue(from, "process") && booleanTrue(to, "process")) {
                //add edge to the parentsOf HasSet
                parentsOf.computeIfAbsent(toId, k -> new HashSet<>()).add(fromId);
            }
            else if (booleanTrue(to, "process")){
                parentsOf.computeIfAbsent(toId, k -> new HashSet<>()).add("stageIn");
            }
        }
        parseIOcsv(inputCsvPath, outputCsvPath);

        //get cpu and memory requirements for each job from the trace.csv file
        //and add the file list while we are already iterating over every task
        try (BufferedReader r = Files.newBufferedReader(Path.of(traceCsvPath))) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                // tolerate header or comments
                if (line.startsWith("#")) continue;
                String[] parts = line.split(",");
                if (parts.length < 2) continue;
                String jobName = parts[6].trim();
                Job j = jobsByName.get(jobName);
                if (j == null) continue;
                j.runtime = Long.parseLong(parts[20].trim()) / 1000L;
                j.cores = Integer.parseInt(parts[11].trim());
                j.memory = Long.parseLong(parts[14].trim());
                j.uses = fileMap.getOrDefault(j.taskName, new ArrayList<>());
            }
        }
        Job stageIn = new Job();
        stageIn.id = "stageIn";
        stageIn.taskName = "stageIn";
        stageIn.uses = getStageInFiles();
        jobs.put(stageIn.id, stageIn);
        jobsByName.put(stageIn.taskName, stageIn);





        // Build ADAG XML document
        Document doc = buildXmlDocument(jobs, parentsOf);

        // Write out XML
        writeXml(doc, Path.of(outPath).toFile());
    }

    private static String pathToName(String path) {
        String name = path.trim().replace("/", "_");

        String[] nameSplit = name.split("\\.");
        if (nameSplit.length < 2) {
            return name;
        } else {
            String newName = nameSplit[0];
            for (int i = 0; i < nameSplit.length - 2; i++) {
                newName += "_" + nameSplit[i + 1];
            }
            newName += "." + nameSplit[nameSplit.length - 1];
            return newName;
        }
    }
    private static void parseIOcsv(String csvPathIn, String csvPathOut) throws IOException {
        try (BufferedReader r = Files.newBufferedReader(Path.of(csvPathIn))) {
            String line = r.readLine();
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                // tolerate header or comments
                if (line.startsWith("#")) continue;
                String[] parts = line.split(";");
                if (parts.length < 4) continue;
                String name = pathToName(parts[2].trim());
                String taskName = parts[0].trim();
                UseFile file = new UseFile();
                file.path = name;
                file.size = Long.parseLong(parts[4].trim());
                file.link = "input";
                List<UseFile> fileList = fileMap.getOrDefault(taskName, new ArrayList<>());
                fileList.add(file);
                fileMap.put(taskName, fileList);
                fileSizesMap.computeIfAbsent(file.path, k -> file.size);
                fileNamesIn.add(name);
                List<String> taskList = nameToTaskNameInput.getOrDefault(name, new ArrayList<>());
                taskList.add(taskName);
                nameToTaskNameInput.put(name, taskList);
//                if (name.equals("_input_data_work_cf_d3af55c8ed10ed1c52467d191e4ba6_TREATMENT_rhIFNb_100_REP1_dup_intercept_mqc.txt")) {
//                    System.out.println(nameToTaskNameOutput.get(name) +", " + jobsByName.get(nameToTaskNameOutput.get(name).getFirst()).id);
//                    System.out.println(nameToTaskNameOutput.get(name));
//                }

            }
        }

        try (BufferedReader r = Files.newBufferedReader(Path.of(csvPathOut))) {
            String line = r.readLine();
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                // tolerate header or comments
                if (line.startsWith("#")) continue;
                String[] parts = line.split(";");
                if (parts.length < 2) continue;
                String name = pathToName(parts[2].trim());
                String taskName = parts[0].trim();
                UseFile file = new UseFile();
                file.path = name;
                file.size = Long.parseLong(parts[4].trim());
                file.link = "output";
                List<UseFile> fileList = fileMap.getOrDefault(taskName, new ArrayList<>());
                fileList.add(file);
                fileMap.put(taskName, fileList);
                fileSizesMap.computeIfAbsent(file.path, k -> file.size);
                fileNamesOut.add(name);
                List<String> taskList = nameToTaskNameOutput.getOrDefault(name, new ArrayList<>());
                taskList.add(taskName);
                nameToTaskNameOutput.put(name, taskList);
//                if (name.equals("_input_data_work_cf_d3af55c8ed10ed1c52467d191e4ba6_TREATMENT_rhIFNb_100_REP1_dup_intercept_mqc.txt")) {
//                    System.out.println(nameToTaskNameOutput.get(name) +", " + jobsByName.get(nameToTaskNameOutput.get(name).getFirst()).id);
//                    System.out.println(nameToTaskNameOutput.get(name));
//                }
            }
        }
    }
//    private static Set<String> getNameList(String csvPath) throws IOException{
//        Set<String> fileNames = new HashSet<>();
//        try (BufferedReader r = Files.newBufferedReader(Path.of(csvPath))) {
//            String line = r.readLine();
//            while ((line = r.readLine()) != null) {
//                line = line.trim();
//                if (line.isEmpty()) continue;
//                // tolerate header or comments
//                if (line.startsWith("#")) continue;
//                String[] parts = line.split(";");
//                if (parts.length < 2) continue;
//                String name = pathToName(parts[2].trim());
//                fileNames.add(name);
//            }
//        }
//        return fileNames;
//    }

    //get all of the files for the stage in job (files that are not generated by jobs but exists at the beginning)
    private static List<UseFile> getStageInFiles() throws IOException {
        List<UseFile> originFiles = new ArrayList<>();


        for (String name : fileNamesIn) {
//            if (name.equals("_input_data_work_cf_d3af55c8ed10ed1c52467d191e4ba6_TREATMENT_rhIFNb_100_REP1_dup_intercept_mqc.txt")) {
//                System.out.println(nameToTaskNameInput.get(name) +", " + jobsByName.get(nameToTaskNameInput.get(name)).id);
//            }
            if (!fileNamesOut.contains(name)) {
                UseFile file = new UseFile();
                file.path = name;
                file.link = "input";
                file.size = fileSizesMap.getOrDefault(name, 1l);
                originFiles.add(file);
            } else {
                List<String> taskNamesIn = nameToTaskNameInput.get(name);
                List<String> taskNamesOut = nameToTaskNameOutput.get(name);
                if (taskNamesOut.size() != 1) {
                    System.out.println("ERROR: " + name + " has multiple producers");
                }

                String idOut = jobsByName.get(taskNamesOut.getFirst()).id;
                for (String taskNameIn : taskNamesIn) {
                    String idIn = jobsByName.get(taskNameIn).id;
                    parentsOf.computeIfAbsent(idIn, k -> new HashSet<>()).add(idOut);
                }

            }
        }


        return originFiles;
    }

//    // Read CSV map taskname -> Files
//    private static Map<String,List<UseFile>> addToFileMapping(String csvPath, Map<String, List<UseFile>> map, String link) throws IOException {
//        try (BufferedReader r = Files.newBufferedReader(Path.of(csvPath))) {
//            String line = r.readLine(); //ignore header
//            while ((line = r.readLine()) != null) {
//                line = line.trim();
//                if (line.isEmpty()) continue;
//                // tolerate header or comments
//                if (line.startsWith("#")) continue;
//                String[] parts = line.split(";");
//                if (parts.length < 2) continue;
//                UseFile file = new UseFile();
//                String taskName= parts[0].trim();
//                String path = parts[2].trim();
//                file.path = pathToName(path);
//                file.size = Long.parseLong(parts[4].trim());
//                file.link = link;
//
//                //get existing File List if exists
//                List<UseFile> fileList = map.getOrDefault(taskName, new ArrayList<>());
//                //add new file to list
//                fileList.add(file);
//                map.put(taskName, fileList);
//                fileSizesMap.computeIfAbsent(file.path, k -> file.size);
//            }
//        }
//        return map;
//    }

    private static boolean booleanTrue(JsonNode node, String field) {
        if (node == null || !node.has(field)) return false;
        JsonNode v = node.get(field);
        return v != null && v.isBoolean() && v.booleanValue();
    }

    private static String nodeIdFor(JsonNode node) {
        if (node == null) return null;
        if (node.has("ID") && !node.get("ID").isNull()) {
            String id = node.get("ID").asText();
            if (id != null && !id.isBlank()) return id;
        }
       throw new IllegalArgumentException("No ID found for node " + node);
    }

    private static Document buildXmlDocument(Map<String, Job> jobs,
                                             Map<String, Set<String>> parentsOf) throws ParserConfigurationException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.newDocument();

        final String DAX_NS = "http://pegasus.isi.edu/schema/DAX";
        final String XSI_NS = "http://www.w3.org/2001/XMLSchema-instance";


        Element adag = doc.createElement("adag");
        adag.setAttribute("name", "rnaseq_real_traces");
        adag.setAttribute("version", "3.3");
        // Namespace-Deklarationen
        adag.setAttribute("xmlns", DAX_NS);
        adag.setAttribute("xmlns:xsi", XSI_NS);
        // schemaLocation mit XSI-Namespace setzen
        adag.setAttributeNS(XSI_NS, "xsi:schemaLocation",
                DAX_NS + " http://pegasus.isi.edu/schema/dax-3.3.xsd");

        doc.appendChild(adag);

        // Jobs
        List<Job> sortedJobs = jobs.values().stream().sorted(Comparator.comparing(Job::getSortId)).toList();
        for (Job j : sortedJobs) {
            Element jobEl = doc.createElement("job");
            jobEl.setAttribute("id", j.id);
            jobEl.setAttribute("name", sanitizeXmlAttr(j.taskName));
            jobEl.setAttribute("runtime", "" + j.runtime);

            Element cores = doc.createElement("profile");
            cores.setAttribute("namespace", j.namespace);
            cores.setAttribute("key", "cores");
            if (j.cores == null) j.cores = 1;
            cores.setTextContent("" + j.cores);
            jobEl.appendChild(cores);

            Element memory = doc.createElement("profile");
            memory.setAttribute("namespace", j.namespace);
            memory.setAttribute("key", "memory");
            memory.setTextContent("" + Math.max(1, j.memory / (1024 * 1024)));
            jobEl.appendChild(memory);

            for (UseFile uf : j.uses) {
                Element uses = doc.createElement("uses");
                uses.setAttribute("file", uf.path);
                uses.setAttribute("link", uf.link);
                uses.setAttribute("transfer", "true");
                uses.setAttribute("register", "false");
                uses.setAttribute("optional", "false");
                uses.setAttribute("size", String.valueOf(uf.size));
                jobEl.appendChild(uses);
            }

            adag.appendChild(jobEl);
        }

        //DAG
        System.out.println("parentsOf: " + parentsOf.size());
        for (String childId : parentsOf.keySet()) {
            Set<String> parents = parentsOf.get(childId);
            Element childEl = doc.createElement("child");
            childEl.setAttribute("ref", childId);
            for (String parent : parents) {
                Element parentEl = doc.createElement("parent");
                parentEl.setAttribute("ref", parent);
                childEl.appendChild(parentEl);
            }
            adag.appendChild(childEl);
        }
        return doc;
    }

    private static String sanitizeXmlAttr(String s) {
        if (s == null) return "";
        return s.replaceAll("[\\x00-\\x1F]", " ").replaceAll("\"", "'");
    }

    private static void writeXml(Document doc, File out) throws Exception {
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer t = tf.newTransformer();
        t.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

        // Add XML comment at top
        DOMSource source = new DOMSource(doc);


        try (FileOutputStream fos = new FileOutputStream(out)) {
            StreamResult result = new StreamResult(new OutputStreamWriter(fos, "UTF-8"));
            t.transform(source, result);
        }
    }
}
