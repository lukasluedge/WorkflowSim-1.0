package org.CWSInterface;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class testMain {

    public static void main(String[] args) throws Exception {
        HttpRunner runner = new HttpRunner("C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\scheduling-000002.json");
        runner.registerScheduler();
        runner.createNodes();
        runner.submitDagVertices();
        runner.submitDagEdges();
        runner.startBatch();
        runner.registerOutputFiles();
        runner.registerTasksSmart();
        runner.endBatch();

        TimeUnit.SECONDS.sleep(1);
//
        Map<Integer, String> result = runner.getNodeAssignmentsForTasks();
        result.forEach((id, node) -> System.out.println("Task " + id + " → Node " + node));

        for (Map.Entry<Integer, String> entry : result.entrySet()) {
            System.out.printf("Task %d liegt auf Node: %s%n", entry.getKey(), entry.getValue());
        }

//        runner.resetCluster();
//
//        runner = new HttpRunner("C:\\Users\\lukas\\IdeaProjects\\WorkflowSim-1.0\\scheduling-000002.json");
//        runner.createNodes();
//        runner.submitDagVertices();
//        runner.submitDagEdges();
//        runner.startBatch();
//        runner.registerTasksSmart();
//        runner.endBatch();
//
//        TimeUnit.SECONDS.sleep(1);
//
//        result = runner.getNodeAssignmentsForTasks();
//        result.forEach((id, node) -> System.out.println("Task " + id + " → Node " + node));
//
//        for (Map.Entry<Integer, String> entry : result.entrySet()) {
//            System.out.printf("Task %d liegt auf Node: %s%n", entry.getKey(), entry.getValue());
//        }

//        runner.resetCluster();




    }
}
