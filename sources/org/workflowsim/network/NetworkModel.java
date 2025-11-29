package org.workflowsim.network;

import java.util.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.FileItem;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.WorkflowSimTags;

public class NetworkModel {
    private static final List<NetworkTransfer> active = new ArrayList<>();
    private static double totalBwMBps = 128; // default e.g. 1 Gbit/s = 128 MB/s — configure as needed
    private static int nextId = 1;
    private static int datacenterIdForEvents = -1; // filled on first use or set via setter

    public static synchronized void setTotalBandwidthMBps(double bw) {
        totalBwMBps = bw;
    }

    public static synchronized void setDatacenterIdForEvents(int dcId) {
        datacenterIdForEvents = dcId;
    }

    public static synchronized NetworkTransfer createAndRegister(double sizeMB, Object payload) {
        double now = CloudSim.clock();
        if (datacenterIdForEvents < 0) {
            // try to find a DC id from payload if it contains Job/VM info — else user must set it
            // but we keep it optional: the caller (WorkflowDatacenter) should call setDatacenterIdForEvents(getId()) once.
        }
        NetworkTransfer nt = new NetworkTransfer(nextId++, sizeMB, payload, now);
        // Before adding, update remaining for current ones to 'now'
        updateRemainingForAll(now);
        active.add(nt);
        redistributeAndRescheduleAll(now);
        return nt;
    }

    public static synchronized void unregister(NetworkTransfer nt) {
        if (nt.unregistered) {
//            System.err.println("Warning: unregistering already unregistered NetworkTransfer");
            return;
        }
        nt.unregistered = true;
        double now = CloudSim.clock();
        // bring all to current time
        updateRemainingForAll(now);
        // mark this as finished so incoming old events are ignored
        nt.finished = true;
        active.remove(nt);
        redistributeAndRescheduleAll(now);
    }

    private static void updateRemainingForAll(double now) {
        for (NetworkTransfer t : active) {
            updateSingleFromNow(t);
            double dt = now - t.lastUpdateTime;
            if (dt > 0 && t.allocatedMBps > 0) {
                double transferred = t.allocatedMBps * dt; // MB
                t.remainingMB = Math.max(0.0, t.remainingMB - transferred);
                t.lastUpdateTime = now;
            } else {
                t.lastUpdateTime = now;
            }
        }
    }

    private static void redistributeAndRescheduleAll(double now) {
        if (active.isEmpty()) {
            return;
        }

        double per = totalBwMBps / active.size();
        for (NetworkTransfer t : active) {
            t.allocatedMBps = per;
        }
//        System.out.printf(
//                "[%8.3f] Active Transfers: %d | BW per transfer: %.2f MB/s\n",
//                CloudSim.clock(), active.size(), per
//        );
        // schedule new completion events (logical cancel: mark old events ignored by ID)
        for (NetworkTransfer t : active) {
            // compute expected remaining duration
            double duration = (t.allocatedMBps > 0) ? (t.remainingMB / t.allocatedMBps) : Double.POSITIVE_INFINITY;
            double eventTime = now + duration;
            // schedule event to datacenter: payload is the NetworkTransfer object itself
            // We rely on logical cancellation: when event arrives, the datacenter should check t.remainingMB <= eps or t.finished flag.

//            System.out.printf(
//                    "[%8.3f] Transfer %s: remaining %.2f MB\n",
//                    CloudSim.clock(), t.id, t.remainingMB            );
            if (datacenterIdForEvents >= 0) {
                CloudSim.send(-1,datacenterIdForEvents, duration, WorkflowSimTags.NETWORK_TRANSFER_COMPLETE, t);
            } else {
                // If datacenter id not set, skip scheduling (caller should set it)
            }
        }
    }

    // convenience to get active transfers (read-only list)
    public static synchronized List<NetworkTransfer> getActiveTransfers() {
        return Collections.unmodifiableList(new ArrayList<>(active));
    }

    // update only the target transfer's remainingMB (invoked before logical-cancel check)
    public static synchronized void updateSingleFromNow(NetworkTransfer t) {
        double now = CloudSim.clock();
        double dt = now - t.lastUpdateTime;
        if (dt > 0 && t.allocatedMBps > 0) {
            double transferred = t.allocatedMBps * dt;
            t.remainingMB = Math.max(0.0, t.remainingMB - transferred);
            t.lastUpdateTime = now;
        } else {
            t.lastUpdateTime = now;
        }
    }
}
