package org.CWSInterface;

import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.workflowsim.WorkflowEngine;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;

/**
 * VmMonitor sampelt periodisch pro VM die CPU- und RAM-Auslastung
 * und schreibt die Daten am Ende der Simulation in eine CSV-Datei.
 *
 * Wichtig: Er beendet sich automatisch, sobald der Workflow abgeschlossen ist,
 * sodass die Simulation nicht durch endlose Sampling-Events offen bleibt.
 *
 * CSV-Spalten:
 *   time,datacenter,hostId,vmId,cpuUtilFraction,ramUsed,ramTotal
 */
public class VmMonitor extends SimEntity {
    private static final int SAMPLE = 10001;

    private final List<Datacenter> datacenters;
    private final WorkflowEngine engine;
    private final double interval;
    private final String outputPath;

    private final List<String> lines = new LinkedList<>();

    // Zur robusten Beendigung: wir warten noch ein paar Samples nach "idle"
    private final int idleSampleThreshold;
    private int idleSamples = 0;
    private boolean stopped = false;

    /**
     * @param name        Entity-Name
     * @param datacenters Zu beobachtende Datacenter
     * @param engine      WorkflowEngine zum Prüfen, ob der Workflow fertig ist
     * @param interval    Sampling-Intervall (Simulationszeit)
     * @param outputPath  Zielpfad der CSV-Datei
     */
    public VmMonitor(String name, List<Datacenter> datacenters, WorkflowEngine engine, double interval, String outputPath) {
        this(name, datacenters, engine, interval, outputPath, 2);
    }

    public VmMonitor(String name, List<Datacenter> datacenters, WorkflowEngine engine, double interval, String outputPath, int idleSampleThreshold) {
        super(name);
        this.datacenters = datacenters;
        this.engine = engine;
        this.interval = interval;
        this.outputPath = outputPath;
        this.idleSampleThreshold = Math.max(0, idleSampleThreshold);
        // CSV-Header
        lines.add("time,datacenter,hostId,vmId,cpuUtilFraction,ramUsed,ramTotal");
    }

    @Override
    public void startEntity() {
        // erstes Sample kurz nach Simulationsbeginn
        schedule(getId(), Math.max(0.1, interval), SAMPLE);
    }

    @Override
    public void processEvent(SimEvent ev) {
        if (ev == null) return;
        int tag = ev.getTag();
        if (tag == CloudSimTags.END_OF_SIMULATION || !CloudSim.running()) {
            // Force stop and avoid scheduling any further samples
            forceStop();
            return;
        }
        if (stopped) return;
        if (tag == SAMPLE) {
            boolean anyVmReported = sampleOnce();

            // Beendigungslogik: Wenn WorkflowEngine fertig ist und keine VMs mehr aktiv sind,
            // zählen wir einige "idle" Samples und stoppen dann endgültig.
            boolean engineDone = isWorkflowFinished();
            boolean vmsIdle = !anyVmReported || allVmsIdle();
            if (engineDone && vmsIdle) {
                idleSamples++;
                if (idleSamples >= idleSampleThreshold) {
                    forceStop();
                    return;
                }
            } else {
                idleSamples = 0;
            }

            // nächstes Sample nur planen, wenn nicht gestoppt
            if (!stopped) {
                schedule(getId(), interval, SAMPLE);
            }
        }
    }

    /**
     * Sampelt einmal alle VMs in allen Datacentern.
     * @return true, wenn mindestens eine VM gesampelt wurde, sonst false
     */
    private boolean sampleOnce() {
        boolean any = false;
        double now = CloudSim.clock();
        for (Datacenter dc : datacenters) {
            for (Host host : dc.getVmAllocationPolicy().getHostList()) {
                int hostId = host.getId();
                for (Vm vm : host.getVmList()) {
                    any = true;
                    double cpuUtil = clamp01(vm.getTotalUtilizationOfCpu(now)); // [0..1]
                    int ramUsed = safeGetRequestedRam(vm);
                    int ramTotal = vm.getRam();

                    StringJoiner sj = new StringJoiner(",");
                    sj.add(String.valueOf(now));
                    sj.add(dc.getName());
                    sj.add(String.valueOf(hostId));
                    sj.add(String.valueOf(vm.getId()));
                    sj.add(String.valueOf(cpuUtil));
                    sj.add(String.valueOf(ramUsed));
                    sj.add(String.valueOf(ramTotal));

                    lines.add(sj.toString());
                }
            }
        }
        return any;
    }

    private boolean isWorkflowFinished() {
        try {
            // Mehr tolerant: Wenn keine offenen Jobs mehr in der Queue sind, betrachten wir den Workflow als beendet.
            // (Manche Engines bereinigen jobsSubmittedList nicht vollständig.)
            return engine.getJobsList().isEmpty();
        } catch (Throwable t) {
            // Fallback: keine Info -> nie "fertig" melden
            return false;
        }
    }

    private boolean allVmsIdle() {
        double now = CloudSim.clock();
        for (Datacenter dc : datacenters) {
            for (Host host : dc.getVmAllocationPolicy().getHostList()) {
                for (Vm vm : host.getVmList()) {
                    double cpu = vm.getTotalUtilizationOfCpu(now);
                    if (cpu > 1e-9) return false;
                }
            }
        }
        return true;
    }

    private static double clamp01(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return 0.0;
        if (v < 0.0) return 0.0;
        return v;
    }

    private static int safeGetRequestedRam(Vm vm) {
        try {
            // Angeforderter RAM; falls VM instanziiert wird, liefert CloudSim typischerweise den vollen VM-RAM
            return vm.getCurrentRequestedRam();
        } catch (Throwable t) {
            return 0;
        }
    }

    /**
     * Stops scheduling, cancels pending SAMPLE events and marks entity as FINISHED.
     */
    private void forceStop() {
        if (stopped) return;
        stopped = true;
        try {
            // Cancel any future SAMPLE events destined from this entity
            CloudSim.cancelAll(getId(), new org.cloudbus.cloudsim.core.predicates.Predicate() {
                @Override
                public boolean match(SimEvent event) {
                    return event.getTag() == SAMPLE;
                }
            });
        } catch (Throwable ignored) {}
        // Mark finished so the scheduler won't run more events for this entity
        try { setState(FINISHED); } catch (Throwable ignored) {}
    }

    @Override
    public void shutdownEntity() {
        // CSV schreiben, wenn Simulation stoppt
        try {
            File out = new File(outputPath);
            File parent = out.getParentFile();
            if (parent != null) parent.mkdirs();

            try (PrintWriter pw = new PrintWriter(out, StandardCharsets.UTF_8)) {
                for (String line : lines) {
                    pw.println(line);
                }
            }
            Log.printLine(getName() + ": wrote VM metrics to " + outputPath);
        } catch (Exception e) {
            Log.printLine(getName() + ": failed to write metrics: " + e.getMessage());
        }
    }
}