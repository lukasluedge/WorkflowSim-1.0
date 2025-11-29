package org.workflowsim.network;

public class NetworkTransfer {
    public final int id;
    public final double totalMB;
    public double remainingMB;
    public double allocatedMBps; // MB/s assigned currently
    public double lastUpdateTime; // CloudSim.clock() when we last updated remainingMB
    public final Object payload; // reference to FileItem or Job + file etc.
    public boolean finished = false;
    public boolean cancelledEvent = false; // for logical cancellation
    public boolean unregistered = false;
    public NetworkTransfer(int id, double totalMB, Object payload, double now) {
        this.id = id;
        this.totalMB = totalMB;
        this.remainingMB = totalMB;
        this.payload = payload;
        this.lastUpdateTime = now;
    }
}
