package org.CWSInterface;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.*;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;
import org.workflowsim.utils.ReplicaCatalog;


public class ExternalSchedulingAlgorithm extends BaseSchedulingAlgorithm {

    private SchedulingSnapshotWriter.ExternalSchedulerConfig cfg;
    private static boolean schedulerRegistered = false;
    private static final java.util.Set<Integer> reportedCompleted = new java.util.HashSet<>();
    public SimEvent ev;
    public static int alreadyDeleted = 0;
    public static boolean first = true;
    public static String execNameWithTimestamp = "my_exec"+'_' + System.currentTimeMillis();
    public ExternalSchedulingAlgorithm(SimEvent ev) {

        super();
        this.ev = ev;
        cfg = new SchedulingSnapshotWriter.ExternalSchedulerConfig()
                .withBaseUrl("http://localhost:8080")
                .withExecution(execNameWithTimestamp)
                .withStrategy("wow")
                .withLocationAware(true)
                .withNamespace("default")
                .withWorkDir("./MockClusterWorkspace")
                .withDns("http://localhost:8080");


    }
    public static final class FileInfo {
        public final String name;
        public final String ioType;
        public final Long sizeBytes; // kann null sein, wenn unbekannt
        public final Integer sizeMB; // kann null sein, wenn unbekannt
        public final List<String> locations;

        public FileInfo(String name, String ioType, Long sizeBytes, Integer sizeMB, List<String> locations) {
            this.name = name;
            this.ioType = ioType;
            this.sizeBytes = sizeBytes;
            this.sizeMB = sizeMB;
            this.locations = locations == null ? Collections.emptyList() : locations;
        }
    }




    @Override
    public void run() throws Exception {
        List<?> cloudlets = getCloudletList();
        List<?> vms = getVmList();


        if (cloudlets == null || vms == null || cloudlets.isEmpty() || vms.isEmpty()) {
            Log.printLine("[SimpleIdleFirst] Keine Cloudlets oder VMs verfügbar.");
            return;
        }
        if (vms.stream().noneMatch(vm -> ((CondorVM) vm).getState() != WorkflowSimTags.VM_STATUS_BUSY)) {
            Log.printLine("[SimpleIdleFirst] Alle VMs sind busy");
            return; // alle VMs sind busy
        }

        List<Cloudlet> finishedCloudlets = Collections.emptyList();
        List<Cloudlet> allFinishedCloudlets = Collections.emptyList();
        List<Integer> finishedCloudletIDs = Collections.emptyList();
        try {
            var dstEntity = CloudSim.getEntity(ev.getDestination());

            if (dstEntity instanceof WorkflowScheduler ws) {
                allFinishedCloudlets = ws.getCloudletReceivedList();
            }
        } catch (Throwable ignored) {}

        // Beispiel: IDs der fertigen Cloudlets loggen
        if (!allFinishedCloudlets.isEmpty()) {
            finishedCloudletIDs = allFinishedCloudlets.stream().map(Cloudlet::getCloudletId).toList();
            System.out.println("[ExternalScheduling] Finished so far: " + finishedCloudletIDs);
        }

        finishedCloudlets = allFinishedCloudlets.subList(alreadyDeleted, allFinishedCloudlets.size());
        alreadyDeleted = allFinishedCloudlets.size();




        String file = SchedulingSnapshotWriter.nextSnapshotFileName();

        SchedulingSnapshotWriter.writeExternalSchedulerSteps(
                "traces/APICalls/" + file,
                getVmList(),           // VMs -> Nodes
                getCloudletList(),     // ready Cloudlets -> Tasks
                getScheduledList(),    // scheduled Cloudlets (für vollständigen DAG)
                finishedCloudlets,
                allFinishedCloudlets,
                cfg
        );
        Log.printLine(String.format("[Scheduling] t=%.3f -> Snapshot geschrieben: %s, numcloudlets: %d",
                CloudSim.clock(), file, cloudlets.size()));




        // Initialize or reuse scheduler registration
        HttpRunner localRunner = new HttpRunner("./traces/APICalls/" + file);

        if (!schedulerRegistered) {localRunner.registerScheduler(); schedulerRegistered = true;}
        localRunner.createNodes();
        localRunner.submitDagVertices();
        localRunner.submitDagEdges();
        localRunner.registerOutputFiles();
        localRunner.startBatch();
        if (!finishedCloudletIDs.isEmpty()) localRunner.reportCompletedTasks();
        localRunner.registerTasksSmart();
        localRunner.endBatch();



        TimeUnit.MILLISECONDS.sleep(500);

        Map<Integer, String> result = localRunner.getNodeAssignmentsForTasks();
        SchedulingSnapshotWriter.writeSchedulingDecisions("./traces/DESC/DESC_" + file, result);



//        int size = getCloudletList().size();
//
//        for (int i = 0; i < size; i++) {
//            Cloudlet cloudlet = (Cloudlet) getCloudletList().get(i);
//            int vmSize = getVmList().size();
//            CondorVM firstIdleVm = null;
//
//            for (int j = 0; j < vmSize; j++) {
//                CondorVM vm = (CondorVM) getVmList().get(j);
//                if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
//                    firstIdleVm = vm;
//                    break;
//                }
//            }
//            if (firstIdleVm == null) {
//                break;
//            }
//
//            for (int j = 0; j < vmSize; j++) {
//                CondorVM vm = (CondorVM) getVmList().get(j);
//                if ((vm.getState() == WorkflowSimTags.VM_STATUS_IDLE)
//                        && (vm.getCurrentRequestedTotalMips() > firstIdleVm.getCurrentRequestedTotalMips())) {
//                    firstIdleVm = vm;
//                }
//            }
//            firstIdleVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
//            cloudlet.setVmId(firstIdleVm.getId());
//            getScheduledList().add(cloudlet);
//        }



        for (Object oCl : cloudlets) {
            Cloudlet cl = (Cloudlet) oCl;
            int clID = cl.getCloudletId();
            String vmIDStr = result.get(clID);
            if (!Objects.equals(vmIDStr, "null")) {
                int vmID = Integer.parseInt(vmIDStr.substring(3));
                CondorVM vm = null;
                for (Object oVm : vms) {
                    vm = (CondorVM) oVm;
                    if (vm.getId() == vmID) {
                        break;
                    }
                }

                if (vm == null) {
                    Log.printLine("[SimpleIdleFirst] Keine IDLE-VM verfügbar; verbleibende Cloudlets warten.");
                    break;
                }

                // Zuweisung: VM auf BUSY, Cloudlet VM setzen, geplante Liste erweitern
//                vm.setState(WorkflowSimTags.VM_STATUS_BUSY);
                cl.setVmId(vm.getId());
                getScheduledList().add(cl);
//                System.out.println(cl.getRequiredFiles());

                Log.printLine(String.format(
                        "[SimpleIdleFirst] Cloudlet %d -> VM %d",
                        cl.getCloudletId(), vm.getId()
                ));
            }
        }
//        SchedulingSnapshotWriter.writeFullSnapshot("./traces/SNAP/SNAP_" + file, cloudlets, vms, getScheduledList());
    }
}

