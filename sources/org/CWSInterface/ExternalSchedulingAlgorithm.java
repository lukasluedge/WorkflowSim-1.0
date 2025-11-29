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
    public SimEvent ev;
    public static int alreadyDeleted = 0;
    public static String execNameWithTimestamp = "my_exec"+'_' + System.currentTimeMillis();
    public boolean la;

    public static void reset() {
        schedulerRegistered = false;
        alreadyDeleted = 0;
        execNameWithTimestamp = "my_exec"+'_' + System.currentTimeMillis();
    }
    public ExternalSchedulingAlgorithm(SimEvent ev, String strategy, boolean locationAware) {

        super();
        this.ev = ev;
        this.la = locationAware;
        cfg = new SchedulingSnapshotWriter.ExternalSchedulerConfig()
                .withBaseUrl("http://localhost:8080")
                .withExecution(execNameWithTimestamp)
                .withStrategy(strategy)
                .withLocationAware(locationAware)
                .withNamespace("default")
                .withWorkDir("./MockClusterWorkspace")
                .withLocalWorkDir("/input")
                .withDns("http://localhost:8080");


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
//            System.out.println("[ExternalScheduling] Finished so far: " + finishedCloudletIDs);
        }

        finishedCloudlets = allFinishedCloudlets.subList(alreadyDeleted, allFinishedCloudlets.size());
        alreadyDeleted = allFinishedCloudlets.size();




        String file = SchedulingSnapshotWriter.nextSnapshotFileName(false);

        SchedulingSnapshotWriter.writeExternalSchedulerSteps(
                "traces/APICalls/" + file,
                getVmList(),           // VMs -> Nodes
                getCloudletList(),     // ready Cloudlets -> Tasks
                getScheduledList(),    // scheduled Cloudlets (für vollständigen DAG)
                finishedCloudlets,
                schedulerRegistered,
                cfg
        );
        Log.printLine(String.format("[Scheduling] t=%.3f -> Snapshot geschrieben: %s, numcloudlets: %d",
                CloudSim.clock(), file, cloudlets.size()));




        // Initialize or reuse scheduler registration
        HttpRunner localRunner = new HttpRunner("./traces/APICalls/" + file, schedulerRegistered);

        if (!schedulerRegistered) {
            localRunner.registerScheduler();
            localRunner.createNodes();
            localRunner.submitDagVertices();
            localRunner.submitDagEdges();
            schedulerRegistered = true;
        }


        if (this.la ) {
            //send output files to scheduler
            localRunner.registerOutputFiles();
            //get requested copies from the scheduler
            List<Map.Entry<String,String>> requestedCopies = localRunner.getRequestedCopiesAsList();
            for (Map.Entry<String,String> e : requestedCopies) {
                String path = e.getKey();
                String vm = e.getValue();

                String filename = path.split("/")[path.split("/").length-1];
                String vmID = vm.split("-")[1];
                //mark the copies as completed in the simulation as there is no way to simulate a background copy task
//                System.out.println("FILE:" + filename + " VM:" + vmID);
                ReplicaCatalog.addFileToStorage(filename, vmID);
            }
//            System.out.println(requestedCopies);
        }
        localRunner.startBatch();
        if (!finishedCloudletIDs.isEmpty()) localRunner.reportCompletedTasks();
        localRunner.registerTasksSmart();
        localRunner.endBatch();


        Map<Integer, String> result = new HashMap<>();
        int numTries = 0;
        while (result.isEmpty()) {
            long time = System.currentTimeMillis();
//            System.out.print("waiting for task mapping for: ");
//          TimeUnit.MILLISECONDS.sleep(300);
            result = localRunner.getTaskToNodeMappingFromScheduler(execNameWithTimestamp);
//            System.out.println((System.currentTimeMillis() - time) + "ms");
//            System.out.println(result.toString());
            numTries++;
            if (numTries > 2) {
                Log.printLine("[SimpleIdleFirst] No task mapping found after 3 tries.");
                break;
            }

        }





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
            String vmIDStr = result.getOrDefault(clID, "null");
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
        SchedulingSnapshotWriter.writeFullSnapshot("./traces/SNAP/SNAP_" + file, cloudlets, vms, getScheduledList());
    }
}

