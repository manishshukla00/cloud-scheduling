package scheduling;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GoogleTraceParser {

    public static List<Cloudlet> createCloudletsFromTrace(String filePath, int brokerId) {
        List<Cloudlet> cloudletList = new ArrayList<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            boolean isFirstLine = true;
            int cloudletId = 0;

            // Regex pattern to extract 'cpus' and 'memory' values from the JSON string
            Pattern cpuPattern = Pattern.compile("'cpus':\\s*([0-9.]+)");
            Pattern memPattern = Pattern.compile("'memory':\\s*([0-9.]+)");

            while ((line = br.readLine()) != null) {
                if (isFirstLine) { // Skip the header line
                    isFirstLine = false;
                    continue;
                }

                // Since the data contains JSON (which might contain commas), 
                // we carefully parse it using Regex to find CPU and Memory directly from the entire line.
                
                try {
                    double cpuRequest = 0.01; // Default value
                    double memRequest = 0.01; // Default value

                    Matcher cpuMatcher = cpuPattern.matcher(line);
                    if (cpuMatcher.find()) {
                        cpuRequest = Double.parseDouble(cpuMatcher.group(1));
                    }

                    Matcher memMatcher = memPattern.matcher(line);
                    if (memMatcher.find()) {
                        memRequest = Double.parseDouble(memMatcher.group(1));
                    }

                    // Convert Google's CPU fraction (e.g., 0.0206) to CloudSim Length (MI)
                    // Assuming 1.0 CPU = 1,000,000 (1 Million) Instructions
                    long length = (long) (cpuRequest * 1000000); 
                    if (length <= 0) length = 1000; // Minimum task length should be 1000 MI

                    // Convert Memory fraction to File Size
                    long fileSize = (long) (memRequest * 100000);
                    if (fileSize <= 0) fileSize = 300;
                    
                    long outputSize = fileSize; 
                    int pesNumber = 1; // 1 CPU core

                    Cloudlet cloudlet = new Cloudlet(cloudletId, length, pesNumber, fileSize, outputSize, 
                                                     new UtilizationModelFull(), 
                                                     new UtilizationModelFull(), 
                                                     new UtilizationModelFull());
                    
                    cloudlet.setUserId(brokerId);
                    cloudletList.add(cloudlet);
                    cloudletId++;

                    // Warning: The Google dataset can have millions of lines. 
                    // To run the simulation quickly, we are only taking 1000 tasks for now.
                    if (cloudletId >= 1000) break;

                } catch (Exception e) {
                    // If the data in a line is corrupted, ignore it and proceed to the next line
                    continue;
                }
            }
            System.out.println("Successfully loaded " + cloudletList.size() + " Cloudlets from Google Trace!");
            
        } catch (Exception e) {
            System.out.println("Error reading Google Trace file: " + e.getMessage());
            e.printStackTrace();
        }
        
        return cloudletList;
    }
}