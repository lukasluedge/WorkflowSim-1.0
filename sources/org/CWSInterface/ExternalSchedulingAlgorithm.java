package org.CWSInterface;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.workflowsim.*;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;
import org.workflowsim.utils.ReplicaCatalog;

/**
 * Data aware algorithm. Schedule a job to a vm that has most input data it requires.
 * It only works for a local environment.
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class ExternalSchedulingAlgorithm extends BaseSchedulingAlgorithm {

    public ExternalSchedulingAlgorithm() {

        super();


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



        SchedulingSnapshotWriter.ExternalSchedulerConfig cfg =
                new SchedulingSnapshotWriter.ExternalSchedulerConfig()
                        .withBaseUrl("http://localhost:8080")
                        .withExecution("my-exec")
                        .withStrategy("fifo-rr")
                        .withLocationAware(false)
                        .withNamespace("default")
                        .withWorkDir("./MockClusterWorkspace")
                        .withDns("http://localhost:8080");

        String file = SchedulingSnapshotWriter.nextSnapshotFileName();



        SchedulingSnapshotWriter.writeExternalSchedulerSteps(
                file,
                getVmList(),           // VMs -> Nodes
                getCloudletList(),     // ready Cloudlets -> Tasks
                getScheduledList(),    // scheduled Cloudlets (für vollständigen DAG)
                cfg
        );
        Log.printLine(String.format("[Scheduling] t=%.3f -> Snapshot geschrieben: %s, numcloudlets: %d",
                CloudSim.clock(), file, cloudlets.size()));



        HttpRunner runner = new HttpRunner(file);
        runner.executeAllSmart();

        TimeUnit.SECONDS.sleep(1);

        Map<Integer, String> result = runner.getNodeAssignmentsForTasks();
        SchedulingSnapshotWriter.writeSchedulingDecisions("DESC_"+file, result);

        runner.killExecution();
        runner.resetCluster();


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
                        if (vm.getState() == WorkflowSimTags.VM_STATUS_BUSY) {
                            Log.printLine(String.format(
                                    "[SimpleIdleFirst] Cloudlet %d -> VM %d (VM bereits BUSY)",
                                    cl.getCloudletId(), vm.getId()
                            ));
                            throw new Exception("Scheduling error VM is busy");
                        }
                        break;
                    }
                }

                if (vm == null) {
                    Log.printLine("[SimpleIdleFirst] Keine IDLE-VM verfügbar; verbleibende Cloudlets warten.");
                    break;
                }

                // Zuweisung: VM auf BUSY, Cloudlet VM setzen, geplante Liste erweitern
                vm.setState(WorkflowSimTags.VM_STATUS_BUSY);
                cl.setVmId(vm.getId());
                getScheduledList().add(cl);

                Log.printLine(String.format(
                        "[SimpleIdleFirst] Cloudlet %d -> VM %d (VM jetzt BUSY)",
                        cl.getCloudletId(), vm.getId()
                ));
            }
        }
        SchedulingSnapshotWriter.writeFullSnapshot("SNAP_"+file, cloudlets, vms, getScheduledList());
    }
}

