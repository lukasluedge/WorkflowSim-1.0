/**
 * Copyright 2012-2013 University Of Southern California
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim.examples;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.util.*;

import org.CWSInterface.ExternalSchedulingAlgorithm;
import org.CWSInterface.HttpRunner;
import org.CWSInterface.SchedulingSnapshotWriter;
import org.CWSInterface.VmMonitor;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.Job;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.network.NetworkModel;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;
import org.workflowsim.utils.Parameters.ClassType;

/**
 * This WorkflowSimExample creates a workflow planner, a workflow engine, and
 * one schedulers, one data centers and 20 vms. You should change daxPath at
 * least. You may change other parameters as well.
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class ExternalSchedulerExample {
    public static List<Integer> savedCopiesPerRun = new ArrayList<>();
    /**
     * Centralized cluster and simulation configuration.
     * Adjust values here to change the cluster, datacenter and VM setup.
     */
    public static class ClusterConfig {
        // High-level cluster knobs
        public int vmNum = 8;              // number of VMs/hosts in the cluster
        public int pePerVm = 32;            // number of CPU cores per VM/host
        public int mipsPerPe = 7000;        // rnaseq cws ceph: bw175 mips7000

        // Host/Datacenter resources
        public int hostRamMb = 128 * 1024; // RAM per host (MB)
        public long hostStorageMb = 960L * 1024; // Storage per host (MB)
        public int hostBw = 1280000;         // Bandwidth per host

        // VM resources (aligned with host so allocation fits)
        public int vmRamMb = 128 * 1024;     // RAM per VM (MB)
        public long vmImageSizeMb = 10_000; // Image size (MB)
        //used with default workflowSim Network model (no real network simulation)
        public long vmBw = 1;            // Bandwidth per VM 10_000
        //used with improved network simulation
        public long networkBwMB = 175;
        public String vmm = "Xen";          // VMM name

        // Datacenter characteristics
        public String arch = "x86";        // system architecture
        public String os = "Linux";        // operating system
        public double timeZone = 1.0;      // time zone
        public double costPerPe = 2.0;      // processing cost (G$/Pe time unit)
        public double costPerMem = 0.02;
        public double costPerStorage = 0.5;
        public double costPerBw = 0.5;

        // Storage characteristics
        public int maxStorageTransferRateMBps = 1000 ; // intra-dc bandwidth for storage 537
        public double storageCapacityBytes = 4_123_168_604_160.0;  // storage capacity for HarddriveStorage

        // DAX path
        public String daxPath = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\rnaseqMock\\orig_traces\\rnaseq_real.xml";
//        daxPath = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\dax\\Montage_100.xml";
//        daxPath = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\rnaseqMock\\rnaseq.xml";
//        daxPath = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\rnaseqMock\\orig_traces\\rnaseq_real.xml";
    }

    private static final ClusterConfig CFG = new ClusterConfig();

    protected static List<CondorVM> createVM(int userId, int vms, int pesNumber, int mips) { // legacy signature kept for compatibility
            return createVM(userId, CFG);
        }

        protected static List<CondorVM> createVM(int userId, ClusterConfig cfg) {
        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<CondorVM> list = new LinkedList<>();

        //VM Parameters from config
        long size = cfg.vmImageSizeMb; // image size (MB)
        int ram = cfg.vmRamMb;         // vm memory (MB)
        long bw = cfg.vmBw;
        String vmm = cfg.vmm; // VMM name

        //create VMs
        CondorVM[] vm = new CondorVM[cfg.vmNum];
        for (int i = 0; i < cfg.vmNum; i++) {
            double ratio = 1.0;
            vm[i] = new CondorVM(i, userId, cfg.mipsPerPe * ratio, cfg.pePerVm, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm[i]);
        }
        return list;
    }

    public static void main(String[] args) throws Exception {
//        String directoryPath = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\dax";
        String directoryPath = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\rnaseqMock\\DAX";
        List<String> excludeFiles = new ArrayList<>();
        excludeFiles.add("C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\rnaseqMock\\DAX\\chipseq_cws-ceph_1_DAX.xml");
        File folder = new File(directoryPath);

    if (false) {

//        simulate_wrapper("rnaseq", "orig-ceph", "orig-ceph");
//        simulate_wrapper("rnaseq", "orig-ceph", "cws-ceph");
//        simulate_wrapper("rnaseq", "cws-ceph", "orig-ceph");
//        simulate_wrapper("rnaseq", "cws-ceph", "cws-ceph");
//
        simulate_wrapper("rnaseq", "cws-ceph", "cws-ceph");
        simulate_wrapper("rnaseq", "cws-ceph", "la-ceph");
        simulate_wrapper("rnaseq", "la-ceph", "cws-ceph");
        simulate_wrapper("rnaseq", "la-ceph", "la-ceph");
//
//        simulate_wrapper("chipseq", "orig-ceph", "orig-ceph");
//        simulate_wrapper("chipseq", "orig-ceph", "cws-ceph");
//        simulate_wrapper("chipseq", "cws-ceph", "orig-ceph");
//        simulate_wrapper("chipseq", "cws-ceph", "cws-ceph");
//
//        simulate_wrapper("chipseq", "cws-ceph", "cws-ceph");
//        simulate_wrapper("chipseq", "cws-ceph", "la-ceph");
//        simulate_wrapper("chipseq", "la-ceph", "cws-ceph");
//        simulate_wrapper("chipseq", "la-ceph", "la-ceph");

//        simulate_wrapper("allIntoOne", "orig-ceph", "orig-ceph");
//        simulate_wrapper("allIntoOne", "orig-ceph", "cws-ceph");
//        simulate_wrapper("allIntoOne", "cws-ceph", "orig-ceph");
//        simulate_wrapper("allIntoOne", "cws-ceph", "cws-ceph");
//
//        simulate_wrapper("allIntoOne", "cws-ceph", "cws-ceph");
//        simulate_wrapper("allIntoOne", "cws-ceph", "la-ceph");
//        simulate_wrapper("allIntoOne", "la-ceph", "cws-ceph");
//        simulate_wrapper("allIntoOne", "la-ceph", "la-ceph");
    }

    if (false) {
        Integer[] bws = {175};
        Integer[] mipss = {15000};
        Integer[] nodess = {2, 4, 8 ,12, 20, 30, 40, 100};
        for (int mips : mipss) {
            for (int bw : bws) {
//                synthetic_wrapper("rnaseq", true, "la-ceph", 8, 32, 128 * 1024, bw, mips);
                synthetic_wrapper("chipseq", true, "cws-ceph", "cws-ceph", 8, 32, 128 * 1024, bw, mips);
                System.out.println("mips: " + mips + "bw: " + bw);
            }
        }
    }

    if (true) {
//        simulate_wrapper("rnaseq", "cws-nfs", "cws-nfs");
        simulate_wrapper("rnaseq", "la-ceph", "la-ceph");
//        simulate_wrapper("allIntoOne", "cws-ceph", "cws-ceph");
//        simulate_wrapper("chipseq", "cws-ceph", "cws-ceph");
//        PrintWriter writer = new PrintWriter("file_transfers.csv", "UTF-8");
//        writer.println("jobID,origTime,new_time");
//        for (List<Double> tuple : WorkflowDatacenter.DebugBuffer) {
//            writer.println(tuple.get(0) + "," + tuple.get(1) + "," + tuple.get(2));
//        }
//        writer.close();
    }

//
//        simulate("C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\rnaseqMock\\DAX\\rnaseq_orig-ceph_1_DAX.xml", Parameters.SchedulingAlgorithm.DATA, true);

    }
    private static void synthetic_wrapper(String workflow, boolean realWf, String strategy_trace, String strategy_sim, int nodes, int cores, int ram, int bw, int mips) {
        String daxPath;
        if (realWf) daxPath = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\rnaseqMock\\DAX\\" + workflow + "_" + strategy_trace + "_1_DAX.xml";
        else daxPath = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\dax\\" + workflow + ".xml";
        CFG.vmNum = nodes;
        CFG.pePerVm = cores;
        CFG.mipsPerPe = mips;
        CFG.vmRamMb = ram;
        CFG.hostRamMb = ram;
        CFG.networkBwMB = bw;
        String outTraceName = "synthetic\\" + workflow + "_" + strategy_sim + "_" + nodes + "_" + cores + "_" + ram + "_" + bw + "_" + mips;

        simulate(daxPath, getSchedulingAlgorithm(strategy_sim), false, outTraceName);
    }
    private static void simulate_wrapper(String workflow, String strategyA, String strategyB) {
        String strategyTrace = strategyA;
        String strategySim = strategyB;
        String fileSystem = strategySim.split("-")[1];
        String numTrace = "1";
        String daxFile ="C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\config\\rnaseqMock\\DAX\\" + workflow + "_" + strategyTrace + "_" + numTrace + "_DAX.xml";
        String outTraceName = workflow + "_" + strategyTrace + "_" + numTrace + "_" + strategySim;
        System.out.println(fileSystem + (fileSystem.equals("nfs")));
        simulate(daxFile, getSchedulingAlgorithm(strategySim), (fileSystem.equals("nfs")), outTraceName);
    }
    private static Parameters.SchedulingAlgorithm getSchedulingAlgorithm(String strategy) {
        if (strategy.contains("-")) {
            strategy = strategy.split("-")[0];
        }
        Parameters.SchedulingAlgorithm parameters = Parameters.SchedulingAlgorithm.DATA;
        switch (strategy) {
            case "cws":
                parameters = Parameters.SchedulingAlgorithm.EXTERNAL_RANK_MAX_FAIR;
                break;
            case "la":
                parameters = Parameters.SchedulingAlgorithm.EXTERNAL_WOW;
                break;
            case "orig":
                parameters = Parameters.SchedulingAlgorithm.EXTERNAL_FIFO;
                break;
            case "data":
                parameters = Parameters.SchedulingAlgorithm.DATA;
        }
        return parameters;
    }

    public static void simulate(String daxPath, Parameters.SchedulingAlgorithm strategy, boolean sharedFS, String outTraceName) {

        Log.setDisabled(false);
        String runName = daxPath.split("\\\\")[daxPath.split("\\\\").length - 1].replace(".xml", "");


        try {

            PrintStream ps = new PrintStream(new FileOutputStream("C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\CWSExperiments\\SimOutput\\logs\\"+ outTraceName + "_" + System.currentTimeMillis() + ".log", false), true, "UTF-8");
            Log.setOutput(ps);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {

            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }

            /**
             * Since we are using MINMIN scheduling algorithm, the planning
             * algorithm should be INVALID such that the planner would not
             * override the result of the scheduler
             */
            Parameters.SchedulingAlgorithm sch_method = strategy;
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
            if (!sharedFS) {
                file_system = ReplicaCatalog.FileSystem.LOCAL;
            }


            /**
             * No overheads
             */
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);

            /**
             * No Clustering
             */
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);

            /**
             * Initialize static parameters
             */
            Parameters.init(CFG.vmNum, daxPath, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0);
            ReplicaCatalog.init(file_system);

            // before creating any entities.
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0", CFG);
            datacenter0.useSharedNetwork = (CFG.networkBwMB > 0);



            /**
             * Create a WorkflowPlanner with one schedulers.
             */
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            /**
             * Create a WorkflowEngine.
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();

            /**
             * Create a list of VMs.The userId of a vm is basically the id of
             * the scheduler that controls this vm.
             */
            List<CondorVM> vmlist0 = createVM(wfEngine.getSchedulerId(0), CFG);
            NetworkModel.setTotalBandwidthMBps(CFG.networkBwMB);

            /**
             * Submits this list of vms to this WorkflowEngine.
             */
            wfEngine.submitVmList(vmlist0, 0);

            /**
             * Binds the data centers with the scheduler.
             */
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);

            // VM/Host-Utilization Monitor aktivieren (CPU/RAM über die Zeit erfassen)
            double samplingInterval = 0.5; // Simulationszeit-Einheiten
            String metricsOut = "C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\CWSExperiments\\SimOutput\\metrics\\"
                    + outTraceName + "_vm_metrics.csv";
            new VmMonitor("VmMonitor", java.util.List.of(datacenter0), wfEngine, samplingInterval, metricsOut);



            CloudSim.startSimulation();
            List<Job> outputList0 = wfEngine.getJobsReceivedList();
//            List<Cloudlet> outCloudlets = wfPlanner.getTaskList()
            CloudSim.stopSimulation();
            printJobList(outputList0);
            String FS = "local";
            if (sharedFS) FS = "shared";
            writeTraceCsv(outputList0, vmlist0, Paths.get("C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\CWSExperiments\\SimOutput\\traces\\"+ outTraceName + "_" + FS + ".csv"));
            HttpRunner localRunner = new HttpRunner("./traces/APICalls/" + "scheduling-000000.json", true);
            localRunner.resetCluster();
            ExternalSchedulingAlgorithm.reset();
            SchedulingSnapshotWriter.ExternalSchedulerConfig.reset();
        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }

    protected static WorkflowDatacenter createDatacenter(String name, ClusterConfig cfg) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store one or more
        //    Machines
        List<Host> hostList = new ArrayList<>();

        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating
        //    a Machine.
        for (int i = 1; i <= cfg.vmNum; i++) {
            List<Pe> peList1 = new ArrayList<>();
            // 3. Create PEs and add these into the list.
            //for a quad-core machine, a list of 4 PEs is required:
            for (int j = 0; j < cfg.pePerVm; j++)
                peList1.add(new Pe(j, new PeProvisionerSimple(cfg.mipsPerPe))); // need to store Pe id and MIPS Rating

            int hostId = 0;
            int ram = cfg.hostRamMb; // host memory (MB)
            long storage = cfg.hostStorageMb; // host storage (MB)
            int bw = cfg.hostBw;
            hostList.add(
                    new Host(
                            hostId,
                            new RamProvisionerSimple(ram),
                            new BwProvisionerSimple(bw),
                            storage,
                            peList1,
                            new VmSchedulerTimeShared(peList1))); // This is our first machine
            //hostId++;
        }

        // 4. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).
        String arch = cfg.arch;      // system architecture
        String os = cfg.os;          // operating system
        String vmm = cfg.vmm;
        double time_zone = cfg.timeZone;         // time zone this resource located
        double cost = cfg.costPerPe;              // the cost of using processing in this resource
        double costPerMem = cfg.costPerMem;       // the cost of using memory in this resource
        double costPerStorage = cfg.costPerStorage; // the cost of using storage in this resource
        double costPerBw = cfg.costPerBw;         // the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<>();	//we are not adding SAN devices by now
        WorkflowDatacenter datacenter = null;

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

        // 5. Finally, we need to create a storage object.
        /**
         * The bandwidth within a data center in MB/s.
         */
        int maxTransferRate = cfg.maxStorageTransferRateMBps;// the number comes from the futuregrid site, you can specify your bw

        try {
            // Here we set the bandwidth according to config
            HarddriveStorage s1 = new HarddriveStorage(name, cfg.storageCapacityBytes);
            s1.setMaxTransferRate(maxTransferRate);
            storageList.add(s1);
            datacenter = new WorkflowDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }

    /**
     * Prints the job objects
     *
     * @param list list of jobs
     */
    protected static void printJobList(List<Job> list) {
        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Job ID" + indent + "Task ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + indent
                + "Time" + indent + "Start Time" + indent + "Finish Time" + indent + "Depth");
        DecimalFormat dft = new DecimalFormat("###.##");
        for (Job job : list) {
            Log.print(indent + job.getCloudletId() + indent + indent);
            if (job.getClassType() == ClassType.STAGE_IN.value) {
                Log.print("Stage-in");
            }
            for (Task task : job.getTaskList()) {
                Log.print(task.getCloudletId() + ",");
            }
            Log.print(indent);

            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");
                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                Log.print("FAILED");
                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            }
        }
    }
    /**
     * Exportiert eine Nextflow-ähnliche trace.csv mit möglichst vielen aus der
     * Simulation ablesbaren Metriken.
     *
     * Belegte Spalten: task_id, hostname, name, status, exit, cpus, attempt,
     * submit, start, complete, duration, realtime.
     * Nicht simulierte Felder werden als "-" ausgegeben.
     *
     * Beispiel-Aufruf nach Simulationsende:
     *   writeTraceCsv(jobs, vmlist, java.nio.file.Paths.get("./traces/sim_trace.csv"));
     *
     * @param jobs  Liste der simulierten Jobs (Cloudlets)
     * @param vms   Liste der VMs aus der Simulation
     * @param out   Zielpfad für die CSV
     */
    public static void writeTraceCsv(List<Job> jobs, List<CondorVM> vms, Path out) throws IOException {
        if (jobs == null) jobs = Collections.emptyList();
        if (vms == null) vms = Collections.emptyList();

        // VM-Index nach ID -> Label
        Map<Integer, String> vmLabel = new HashMap<>();
        for (CondorVM vm : vms) {
            String label = "vm-" + vm.getId();
            vmLabel.put(vm.getId(), label);
        }

        // Header gemäß Nextflow trace (alle möglichen Felder, nicht belegte = "-")
        String header = String.join(",",
                "task_id","hostname","native_id","hash","process","tag","name","status","exit","module","container",
                "cpus","time","disk","memory","attempt","submit","start","complete","duration","realtime","queue",
                "%cpu","%mem","rss","vmem","peak_rss","peak_vmem","rchar","wchar","syscr","syscw",
                "read_bytes","write_bytes","vol_ctxt","inv_ctxt","workdir","scratch","error_action"
        );

        StringBuilder sb = new StringBuilder(64_000);
        sb.append(header).append('\n');

        for (Job j : jobs) {
            Task t;
            try {
                t = j.getTaskList().getFirst();
            } catch (Exception e) {
                t = (Task) j;
            }
            Cloudlet c = (Cloudlet) t;

            int id = j.getCloudletId();
            String hostname = "-";
            if (j.getVmId() != -1) {
                hostname = vmLabel.getOrDefault(j.getVmId(), "vm-" + j.getVmId());
            }

            // Name/Process soweit möglich
            String name = null;
            try {
                // WorkflowSim Job hat oft einen Namen (sonst fallback)
                name = t.getType();
            } catch (Throwable ignored) {}
            if (name == null || name.isBlank()) {
                name = "task_" + id;
            }
            String process = "-";
            String module = "-";
            String container = "-";

            // CPUs
            int cpus = 1;
            try { cpus = Math.max(1, j.getNumberOfPes()); } catch (Throwable ignored) {}

            // Zeiten (CloudSim nutzt Sekunden)
            double submitSec = 0.0, startSec = 0.0, finishSec = 0.0, cpuTimeSec = 0.0;
            try { submitSec = j.getSubmissionTime(); } catch (Throwable ignored) {}
            try { startSec  = j.getExecStartTime(); } catch (Throwable ignored) {}
            try { finishSec = j.getFinishTime(); } catch (Throwable ignored) {}
            try { cpuTimeSec = j.getActualCPUTime(); } catch (Throwable ignored) {}
            long submitMs = (long) Math.max(0, Math.round(submitSec * 1000.0));
            long startMs  = (long) Math.max(0, Math.round(startSec  * 1000.0));
            long compMs   = (long) Math.max(0, Math.round(finishSec * 1000.0));
            long durationMs = (compMs > 0 && startMs > 0 && compMs >= startMs) ? (compMs - startMs) : 0L;
            long realtimeMs = (long) Math.max(0, Math.round(cpuTimeSec * 1000.0));
            if (realtimeMs <= 0 && durationMs > 0) realtimeMs = durationMs;


            // Status / Exitcode
            String status = j.getCloudletStatusString();
            int exit = ("COMPLETED".equalsIgnoreCase(status) || "SUCCESS".equalsIgnoreCase(status) || "SUCCESSFUL".equalsIgnoreCase(status) || "Success".equalsIgnoreCase(status)) ? 0 : 1;

            // Ressourcenvorgaben: Memory (Bytes), falls im Task hinterlegt
            String memoryBytesStr = "-";
            long memoryBytes = -1L;

            // I/O-Bytes (aus Task.getFileList) – nur wenn klar input/output erkennbar
            long readBytes = 0L;
            long writeBytes = 0L;

            // Zusätzliche Felder, wenn dieses Job-Objekt ein org.workflowsim.Task ist
            try {
                if (t.getType() != null && !t.getType().isBlank()) {
                    process = t.getType();
                }
                // Memory (MB -> Bytes), falls gesetzt
                long memMB = t.getMemoryRequirementMB();
                if (memMB > 0) {
                    memoryBytes = memMB * 1024L * 1024L;
                    memoryBytesStr = Long.toString(memoryBytes);
                }
                // File I/O
                List<?> files = t.getFileList();
                if (files != null && !files.isEmpty()) {
                    for (Object fo : files) {
                        if (fo == null) continue;
                        long size = 0L;
                        try {
                            Method mSz = fo.getClass().getMethod("getSize");
                            Object v = mSz.invoke(fo);
                            if (v instanceof Number n) size = n.longValue();
                        } catch (Throwable ignored) {}

                        // Link/Typ erkennen: getLink() oder getType() oder isInput()/isOutput()
                        String link = null;
                        try {
                            Method mLink = fo.getClass().getMethod("getLink");
                            Object v = mLink.invoke(fo);
                            if (v != null) link = String.valueOf(v);
                        } catch (Throwable ignored) {}
                        if (link == null) {
                            try {
                                Method mType = fo.getClass().getMethod("getType");
                                Object v = mType.invoke(fo);
                                if (v != null) link = String.valueOf(v);
                            } catch (Throwable ignored) {}
                        }
                        if (link == null) {
                            try {
                                Method mIsIn = fo.getClass().getMethod("isInput");
                                Object v = mIsIn.invoke(fo);
                                if (v instanceof Boolean b && b) link = "input";
                            } catch (Throwable ignored) {}
                        }
                        if (link == null) {
                            try {
                                Method mIsOut = fo.getClass().getMethod("isOutput");
                                Object v = mIsOut.invoke(fo);
                                if (v instanceof Boolean b && b) link = "output";
                            } catch (Throwable ignored) {}
                        }

                        if (link != null) {
                            String L = link.toLowerCase(Locale.ROOT);
                            if (L.contains("in")) readBytes += Math.max(0L, size);
                            else if (L.contains("out")) writeBytes += Math.max(0L, size);
                        }
                    }
                }
            } catch (Throwable ignored) {}



            // Cloudlet-Länge (MI) als mögliches "time"-Limit? In Nextflow ist "time" jedoch ein Limit – hier nicht simuliert.
            String timeLimitStr = "-";

            // "disk": nicht als Peak-Disk simuliert – ggf. total output als Annäherung (optional).
            String diskStr = "-";

            // read/write nur setzen, wenn >0 – sonst "-"
            String readStr  = (readBytes  > 0) ? Long.toString(readBytes)  : "-";
            String writeStr = (writeBytes > 0) ? Long.toString(writeBytes) : "-";

            String attempt = "1"; // Standard: keine Retries
            String dash = "-";

            List<String> row = new ArrayList<>(40);
            row.add(Integer.toString(id));      // task_id
            row.add(hostname);                  // hostname
            row.add(dash);                      // native_id
            row.add(dash);                      // hash
            row.add(process);                   // process
            row.add(dash);                      // tag
            row.add(name);                      // name
            row.add(status);                    // status
            row.add(Integer.toString(exit));    // exit
            row.add(module);                    // module
            row.add(container);                 // container
            row.add(Integer.toString(cpus));    // cpus
            row.add(timeLimitStr);              // time (Limit)
            row.add(dash);                      // disk (Peak-Disk nicht simuliert)
            row.add(memoryBytesStr);            // memory (Bytes), falls verfügbar
            row.add(attempt);                   // attempt
            row.add(Long.toString(submitMs));   // submit
            row.add(Long.toString(startMs));    // start
            row.add(Long.toString(compMs));     // complete
            row.add(Long.toString(durationMs)); // duration
            row.add(Long.toString(realtimeMs)); // realtime
            row.add(dash);                      // queue
            row.add(dash);                      // %cpu
            row.add(dash);                      // %mem
            row.add(dash);                      // rss
            row.add(dash);                      // vmem
            row.add(dash);                      // peak_rss
            row.add(dash);                      // peak_vmem
            row.add(dash);                      // rchar
            row.add(dash);                      // wchar
            row.add(dash);                      // syscr
            row.add(dash);                      // syscw
            row.add(readStr);                   // read_bytes
            row.add(writeStr);                  // write_bytes
            row.add(dash);                      // vol_ctxt
            row.add(dash);                      // inv_ctxt
            row.add(dash);                      // workdir
            row.add(dash);                      // scratch
            row.add(dash);                      // error_action

            sb.append(toCsvRow(row)).append('\n');

        }

        Files.createDirectories(out.getParent());
        try (BufferedWriter bw = Files.newBufferedWriter(
                out,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            bw.write(sb.toString());
        }
    }

    private static String toCsvRow(List<String> cols) {
        StringBuilder r = new StringBuilder(cols.size() * 16);
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) r.append(',');
            String c = cols.get(i);
            if (c == null) c = "-";
            boolean needsQuote = c.indexOf(',') >= 0 || c.indexOf('"') >= 0 || c.indexOf('\n') >= 0 || c.indexOf('\r') >= 0;
            if (needsQuote) {
                r.append('"').append(c.replace("\"", "\"\"")).append('"');
            } else {
                r.append(c);
            }
        }
        return r.toString();
    }
}


