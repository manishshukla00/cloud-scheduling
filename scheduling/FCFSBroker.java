package scheduling;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Vm;

public class FCFSBroker extends DatacenterBroker {
    private String schedulingType;

    public FCFSBroker(String name, String type) throws Exception {
        super(name);
        this.schedulingType = type.toUpperCase();
    }

    @Override
    protected void submitCloudlets() {
        List<Cloudlet> clList = getCloudletList();
        List<Vm> vList = getVmList();

        // 1. Sorting Logic: Sort the list according to the algorithm
        if (schedulingType.equals("SJF") || schedulingType.equals("MINMIN")) {
            clList.sort(Comparator.comparingLong(Cloudlet::getCloudletLength));
        } 
        else if (schedulingType.equals("MAXMIN")) {
            clList.sort((c1, c2) -> Long.compare(c2.getCloudletLength(), c1.getCloudletLength()));
        } 
        else if (schedulingType.equals("PRIORITY")) {
            clList.sort((c1, c2) -> Integer.compare(c2.getNumberOfPes(), c1.getNumberOfPes()));
        }
        // No sorting required for FCFS (Default ID order)

        // 2. Binding Logic: Distribute tasks to VMs smartly
        int vmCount = vList.size();
        for (int i = 0; i < clList.size(); i++) {
            Cloudlet cloudlet = clList.get(i);
            
            // Here we ensure that VMs with different capacities are utilized properly
            // In Min-Min, smaller tasks to smaller VMs; in Max-Min, larger tasks to larger VMs
            int targetVmIndex;
            
            if (schedulingType.equals("MINMIN")) {
                // Priority to the VM with the lowest MIPS (so that larger VMs remain free)
                targetVmIndex = i % (vmCount / 2); 
            } else if (schedulingType.equals("MAXMIN")) {
                // Priority to the VM with the highest MIPS
                targetVmIndex = vmCount - 1 - (i % (vmCount / 2));
            } else {
                // Standard Round Robin for FCFS/SJF
                targetVmIndex = i % vmCount;
            }

            bindCloudletToVm(cloudlet.getCloudletId(), vList.get(targetVmIndex).getId());
        }

        super.submitCloudlets();
    }
}