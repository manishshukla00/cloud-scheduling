package scheduling;

import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;

import java.util.List;
import java.util.Random;

public class PSOBroker extends DatacenterBroker {

    // Main PSO parameters (You can change and test these for your research paper)
    private int swarmSize = 30; // Number of particles (Swarm size)
    private int maxIterations = 100; // Number of times the algorithm will run
    private double w = 0.729; // Inertia weight
    private double c1 = 1.49445; // Cognitive coefficient 
    private double c2 = 1.49445; // Social coefficient 

    public PSOBroker(String name) throws Exception {
        super(name);
    }

    @Override
    protected void submitCloudlets() {
        List<Cloudlet> clList = getCloudletList();
        List<Vm> vmList = getVmsCreatedList();

        int numTasks = clList.size();
        int numVms = vmList.size();

        // If there are no tasks or VMs, do not proceed
        if (numTasks == 0 || numVms == 0) {
            System.out.println("PSO Broker: The list of Cloudlets or VMs is empty!");
            return;
        }

        // 1. Run the PSO algorithm to find the best mapping
        int[] bestMapping = runPSO(clList, vmList, numTasks, numVms);

        // 2. Bind the Cloudlets to their assigned VMs
        for (int i = 0; i < numTasks; i++) {
            Cloudlet cloudlet = clList.get(i);
            int assignedVmIndex = bestMapping[i];
            
            // Index check for safety
            if (assignedVmIndex < 0) assignedVmIndex = 0;
            if (assignedVmIndex >= numVms) assignedVmIndex = numVms - 1;

            int assignedVmId = vmList.get(assignedVmIndex).getId();
            
            // Fixing the Cloudlet to the VM
            bindCloudletToVm(cloudlet.getCloudletId(), assignedVmId);
        }

        System.out.println("PSO Broker successfully mapped " + numTasks + " Cloudlets to " + numVms + " VMs.");
        
        // 3. Submit tasks to the simulator
        super.submitCloudlets();
    }

    // --- Actual Mathematical Logic of PSO ---
    private int[] runPSO(List<Cloudlet> cloudlets, List<Vm> vms, int numTasks, int numVms) {
        Random rand = new Random();
        
        // Creating the Swarm: Each particle is a potential scheduling solution
        double[][] positions = new double[swarmSize][numTasks];
        double[][] velocities = new double[swarmSize][numTasks];
        double[][] pBestPositions = new double[swarmSize][numTasks];
        double[] pBestFitness = new double[swarmSize];
        
        double[] gBestPosition = new double[numTasks];
        double gBestFitness = Double.MAX_VALUE; 

        // 1. Initialize the Swarm
        for (int i = 0; i < swarmSize; i++) {
            for (int j = 0; j < numTasks; j++) {
                // Randomly assign a task to any VM (0 to numVms-1)
                positions[i][j] = rand.nextInt(numVms); 
                velocities[i][j] = (rand.nextDouble() * 2 - 1) * numVms; // Random velocity
                pBestPositions[i][j] = positions[i][j];
            }
            // Check the fitness (Makespan) of this particle
            pBestFitness[i] = calculateMakespan(positions[i], cloudlets, vms);
            
            // Is this the best so far (Global Best)?
            if (pBestFitness[i] < gBestFitness) {
                gBestFitness = pBestFitness[i];
                System.arraycopy(positions[i], 0, gBestPosition, 0, numTasks);
            }
        }

        // 2. Main PSO loop (Iterations)
        for (int iter = 0; iter < maxIterations; iter++) {
            for (int i = 0; i < swarmSize; i++) {
                for (int j = 0; j < numTasks; j++) {
                    double r1 = rand.nextDouble();
                    double r2 = rand.nextDouble();

                    // Update velocity (speed and direction)
                    velocities[i][j] = w * velocities[i][j] 
                                     + c1 * r1 * (pBestPositions[i][j] - positions[i][j]) 
                                     + c2 * r2 * (gBestPosition[j] - positions[i][j]);

                    // Update position (new VM assignment)
                    positions[i][j] = positions[i][j] + velocities[i][j];

                    // Boundary condition: If the value goes out of bounds, bring it back inside
                    if (positions[i][j] < 0) positions[i][j] = 0;
                    if (positions[i][j] > numVms - 1) positions[i][j] = numVms - 1;
                }

                // Calculate the new fitness 
                double currentFitness = calculateMakespan(positions[i], cloudlets, vms);

                // Update Personal Best
                if (currentFitness < pBestFitness[i]) {
                    pBestFitness[i] = currentFitness;
                    System.arraycopy(positions[i], 0, pBestPositions[i], 0, numTasks);
                }

                // Update Global Best
                if (currentFitness < gBestFitness) {
                    gBestFitness = currentFitness;
                    System.arraycopy(positions[i], 0, gBestPosition, 0, numTasks);
                }
            }
        }

        // 3. Convert the best mapping to integers and return it
        int[] finalMapping = new int[numTasks];
        for (int j = 0; j < numTasks; j++) {
            finalMapping[j] = (int) Math.round(gBestPosition[j]);
            // Double check: Index should not go out of bounds
            if (finalMapping[j] >= numVms) finalMapping[j] = numVms - 1;
            if (finalMapping[j] < 0) finalMapping[j] = 0;
        }

        return finalMapping;
    }

    // Fitness function: Calculate Makespan (when the last task will finish)
    private double calculateMakespan(double[] position, List<Cloudlet> cloudlets, List<Vm> vms) {
        double[] vmFinishTimes = new double[vms.size()];

        for (int i = 0; i < cloudlets.size(); i++) {
            int vmIndex = (int) Math.round(position[i]);
            
            // Index check for safety
            if(vmIndex >= vms.size()) vmIndex = vms.size() - 1;
            if(vmIndex < 0) vmIndex = 0;

            Cloudlet task = cloudlets.get(i);
            Vm vm = vms.get(vmIndex);

            // Time required to complete this task = Task Length / VM MIPS
            double executionTime = task.getCloudletLength() / vm.getMips();
            
            // Add to the total time of that VM
            vmFinishTimes[vmIndex] += executionTime;
        }

        // Makespan = The maximum time taken among all VMs
        double makespan = 0;
        for (double time : vmFinishTimes) {
            if (time > makespan) {
                makespan = time;
            }
        }
        return makespan;
    }
}