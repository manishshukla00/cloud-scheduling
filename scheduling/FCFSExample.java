package scheduling;

import java.io.File;
import java.util.*;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;

public class FCFSExample {

    static class SimulationStats {
        String algoName;
        double avgWaitingTime;
        double avgTurnaroundTime;
        double makespan;

        SimulationStats(String name, double awt, double atat, double ms) {
            this.algoName = name;
            this.avgWaitingTime = awt;
            this.avgTurnaroundTime = atat;
            this.makespan = ms;
        }
    }

    public static void main(String[] args) {
        // 'PSO' is included in the list of algorithms
        String[] algorithms = {"FCFS", "SJF", "MINMIN", "MAXMIN", "PRIORITY", "PSO"};
        List<SimulationStats> finalComparison = new ArrayList<>();

        for (String algo : algorithms) {
            try {
                // It is necessary to reset CloudSim before each simulation
                CloudSim.init(1, Calendar.getInstance(), false);
                
                // Creating a Datacenter with sufficient resources
                createDatacenter("Datacenter_" + algo);
                
                DatacenterBroker broker;
                
                // Choose the correct broker based on the algorithm
                if (algo.equals("PSO")) {
                    broker = new PSOBroker("Broker_" + algo);
                } else {
                    broker = new FCFSBroker("Broker_" + algo, algo);
                }
                
                int brokerId = broker.getId();

                // Research standard: Creating 20 Virtual Machines
                List<Vm> vmList = new ArrayList<>();
                for (int i = 0; i < 20; i++) {
                    int mips = (i % 2 == 0) ? 1000 : 500; // Heterogeneous VMs
                    vmList.add(new Vm(i, brokerId, mips, 1, 1024, 1000, 10000, "Xen", new CloudletSchedulerSpaceShared()));
                }
                broker.submitVmList(vmList);

                // --- Loading Google Traces Dataset ---
                // Enter the correct path of your Google Traces file here
                String csvPath = "C:\\Users\\hp\\eclipse-workspace\\CloudSimScheduling\\src\\scheduling\\google_traces_data.csv";
                
                File file = new File(csvPath);
                if (!file.exists()) {
                    System.err.println("\n[ERROR] Google Traces file not found! Please check the path: " + csvPath);
                    return; // Stop the program here if the file does not exist
                }

                // Loading Cloudlets using the new parser
                List<Cloudlet> cloudletList = GoogleTraceParser.createCloudletsFromTrace(csvPath, brokerId);
                
                if (cloudletList.isEmpty()) {
                    System.err.println("\n[ERROR] No Cloudlet loaded from the dataset!");
                    return;
                }

                // Apply sorting only for list-based algorithms (PSO applies its own logic)
                if (!algo.equals("PSO")) {
                    applySorting(cloudletList, algo);
                }
                
                broker.submitCloudletList(cloudletList);

                // Start simulation
                CloudSim.startSimulation();
                
                // Get the results
                List<Cloudlet> resultList = broker.getCloudletReceivedList();
                CloudSim.stopSimulation();

                // Calculation: Makespan, Waiting Time, and Turnaround Time
                double totalWT = 0, totalTAT = 0, maxFinish = 0;
                
                for (Cloudlet c : resultList) {
                    double tat = c.getFinishTime() - c.getSubmissionTime();
                    double actualExecTime = c.getActualCPUTime(); 
                    double wt = tat - actualExecTime;
                    
                    totalTAT += tat;
                    totalWT += Math.max(0, wt);
                    
                    if (c.getFinishTime() > maxFinish) {
                        maxFinish = c.getFinishTime();
                    }
                }

                if (resultList.size() > 0) {
                    finalComparison.add(new SimulationStats(algo, totalWT / resultList.size(), totalTAT / resultList.size(), maxFinish));
                    System.out.println(">>> Algorithm " + algo + " Simulation Completed.");
                } else {
                    System.err.println("Error: " + algo + " did not process any cloudlets.");
                }

            } catch (Exception e) {
                System.err.println("Error in " + algo + " simulation:");
                e.printStackTrace();
            }
        }

        // Display table
        printFinalReport(finalComparison);
    }

    private static void printFinalReport(List<SimulationStats> stats) {
        System.out.println("\n\n" + "=".repeat(85));
        System.out.println("         FINAL RESEARCH COMPARISON REPORT (CloudSim 3.0 - Google Traces, 20 VMs)");
        System.out.println("=".repeat(85));
        System.out.printf("%-12s | %-18s | %-20s | %-15s\n", "ALGORITHM", "AVG WAITING (s)", "AVG TURNAROUND (s)", "MAKESPAN (s)");
        System.out.println("-".repeat(85));
        for (SimulationStats s : stats) {
            System.out.printf("%-12s | %-18.4f | %-20.4f | %-15.4f\n", s.algoName, s.avgWaitingTime, s.avgTurnaroundTime, s.makespan);
        }
        System.out.println("=".repeat(85));
    }

    private static void applySorting(List<Cloudlet> list, String algo) {
        if (list == null || list.isEmpty()) return;

        switch (algo) {
            case "SJF": case "MINMIN":
                list.sort(Comparator.comparingLong(Cloudlet::getCloudletLength));
                break;
            case "MAXMIN":
                list.sort((c1, c2) -> Long.compare(c2.getCloudletLength(), c1.getCloudletLength()));
                break;
            case "PRIORITY":
                list.sort((c1, c2) -> Integer.compare(c2.getNumberOfPes(), c1.getNumberOfPes()));
                break;
        }
    }

    private static Datacenter createDatacenter(String name) throws Exception {
        List<Host> hostList = new ArrayList<>();
        List<Pe> peList = new ArrayList<>();
        
        int totalMips = 50000; 
        peList.add(new Pe(0, new PeProvisionerSimple(totalMips)));

        hostList.add(new Host(
            0, 
            new RamProvisionerSimple(65536), 
            new BwProvisionerSimple(1000000), 
            10000000, 
            peList, 
            new VmSchedulerTimeShared(peList)
        ));

        return new Datacenter(name, new DatacenterCharacteristics("x86","Linux","Xen",hostList,10.0,3.0,0.05,0.001,0.0), 
                              new VmAllocationPolicySimple(hostList), new LinkedList<>(), 0);
    }
}