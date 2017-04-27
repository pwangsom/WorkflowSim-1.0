/**
 * Copyright 2012-2013 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim.examples.scheduling.mapreduce;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.CloudletSchedulerDynamicWorkload;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.util.DisplayUtil;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.JobScheduledResult;
import org.workflowsim.utils.MapReduceParameter;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;

/**
 * This DynamicWorkloadExample1 uses specifically
 * CloudletSchedulerDynamicWorkload as the local scheduler;
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Oct 13, 2013
 */
public class AMapReduceSchedulingAlgorithmExample {	

    // String daxPath = "/Users/weiweich/NetBeansProjects/WorkflowSim-1.0/config/dax/Montage_100.xml";
    
    // C:\Users\Dell\git\WorkflowSim-1.0\config\dax
	// C://Users/Dell/git
	
    // D:\Users-Profiles\Peerasak
	// D://Users-Profiles/Peerasak/git
	
    // protected static String daxPath = "C://Users/Dell/git/WorkflowSim-1.0/config/dax/mapreduce/mapreduce_16J_64MB.xml";
    // protected static String daxPath = "D://Users-Profiles/Peerasak/git/WorkflowSim-1.0/config/dax/mapreduce/mapreduce_16J_64MB.xml";
    // protected static String daxPath = "D://Users-Profiles/Peerasak/git/WorkflowSim-1.0/config/dax/mapreduce/mapreduce_random_16map_2reduce_64chunk_1024data.xml";

    // protected static int hostNum = 8;
    // protected static int vmNum = 16;

    protected static List<CondorVM> createVM(int userId, int vms) {

        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<CondorVM> list = new LinkedList<>();

        //VM Parameters
        long size = 10000; //image size (MB)
        int ram = 512; //vm memory (MB)
        int mips = 1000;
        long bw = 1000;
        int pesNumber = 1; //number of cpus
        String vmm = "Xen"; //VMM name

        //create VMs
        CondorVM[] vm = new CondorVM[vms];

        for (int i = 0; i < vms; i++) {
            double ratio = 1.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm, new CloudletSchedulerDynamicWorkload(mips * ratio, pesNumber));
            list.add(vm[i]);
        }

        return list;
    }

    ////////////////////////// STATIC METHODS ///////////////////////
    /**
     * Creates main() to run this example This example has only one datacenter
     * and one storage
     */
    public static void main(String[] args) {

        try {
            // First step: Initialize the WorkflowSim package. 

            /**
             * However, the exact number of vms may not necessarily be vmNum If
             * the data center or the host doesn't have sufficient resources the
             * exact vmNum would be smaller than that. Take care.
             */

            /**
             * Should change this based on real physical path
             */
        	
        	String daxPath = "C://Users/Dell/git/WorkflowSim-1.0/mapreduce/input/";
        	// String daxPath = "D://Users-Profiles/Peerasak/git/WorkflowSim-1.0/mapreduce/input/";
        	
        	String fileName = "mr_1000m_16r_v2.xml";
        	
        	daxPath = daxPath + fileName;
        	
        	Log.printLine(daxPath);
            
            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }

            /**
             * Since we are using HEFT planning algorithm, the scheduling
             * algorithm should be static such that the scheduler would not
             * override the result of the planner
             */
            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.DL_HO_4;
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.LOCAL;

            /**
             * No overheads
             */
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);

            /**
             * No Clustering
             */
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);

            /**
             * Initialize static parameters
             */
            Parameters.init(MapReduceParameter.NO_VMS, daxPath, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0);
            ReplicaCatalog.init(file_system);

            // before creating any entities.
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0");

            /**
             * Create a WorkflowPlanner with one schedulers.
             */
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            /**
             * Create a WorkflowEngine.
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            wfEngine.setDfs(true);
            
            /**
             * Create a list of VMs.The userId of a vm is basically the id of
             * the scheduler that controls this vm.
             */
            List<CondorVM> vmlist0 = createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum());
            
            /**
             * Submits this list of vms to this WorkflowEngine.
             */
            wfEngine.submitVmList(vmlist0, 0);

            /**
             * Binds the data centers with the scheduler.
             */
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);
            // wfEngine.getScheduler(wfEngine.getSchedulerId(0)).setDfs(true);

            CloudSim.startSimulation();
            List<Job> outputList0 = wfEngine.getJobsReceivedList();
            CloudSim.stopSimulation();
            
            // DisplayUtil.displayScheduleResultList(wfEngine.getScheduler(0).getCloudletScheduleResultList(), outputList0);
            // DisplayUtil.displayTaskScheduledResultList(wfEngine.getScheduler(0).getTaskScheduledResultList(), outputList0);
            // DisplayUtil.displayExtendedCloudletList(outputList0);
            // DisplayUtil.displayTimeOfJob(outputList0);
            // DisplayUtil.displayJobScheduledResultList(wfEngine.getScheduler(0).getJobScheduledResultList());
            

            Log.printLine();
            Log.printLine("======================================= Simulation Report =======================================");
            Log.printLine();
            Log.printLine("Algorithm Name: " + sch_method);
            Log.printLine("File Name: " + fileName);
            Log.printLine();        
           
            DisplayUtil.displayJobScheduledResultList(createJobScheduledResultList(wfEngine.getScheduler(0).getScheduledJob(), outputList0));
            // DisplayUtil.displayJobScheduledResultListShort(createJobScheduledResultList(wfEngine.getScheduler(0).getScheduledJob(), outputList0));

            Log.printLine();        
            Log.printLine("======================================= Simulation Report =======================================");
            
        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
            e.printStackTrace();
        }
    }
    
    private static JobScheduledResult createJobScheduledResultList(List<Integer> scheduledJobList, List<Job> outputJobList){
    	JobScheduledResult listManager = new JobScheduledResult();
    	
    	Log.printLine("Final scheduledJobList size " + scheduledJobList.size());
    	
    	int count = 0;
    	
    	for(Integer i : scheduledJobList){
    		for(Job job : outputJobList){
    			if(job.getCloudletId() == i){
    				// DisplayUtil.displayJobProperties(job);
    				count++;
    				// Log.printLine("CloudletId " + job.getCloudletId() + "; Matching No. " + count);
    				listManager.add(new JobScheduledResult(job));
    				outputJobList.remove(job);
    				break;
    			}
    		}
    	}
    	
    	return listManager;
    }
    
    protected static WorkflowDatacenter createDatacenter(String name) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store one or more
        //    Machines
        List<Host> hostList = new ArrayList<>();

        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating
        //    a Machine.

        int mips = 2000;
        int ram = 2048; //host memory (MB)
        long storage = 1000000; //host storage
        int bw = 10000;
        
        for(int i = 0; i < MapReduceParameter.NO_HOSTS; i++){
            List<Pe> peList0 = new ArrayList<>();
            
            // 3. Create PEs and add these into the list.
            //for a quad-core machine, a list of 4 PEs is required:
            peList0.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
            peList0.add(new Pe(1, new PeProvisionerSimple(mips)));
            
            hostList.add(
                    new Host(
                            i,
                            new RamProvisionerSimple(ram),
                            new BwProvisionerSimple(bw),
                            storage,
                            peList0,
                            new VmSchedulerTimeShared(peList0))); // This is our first machine        	
        }

        // 4. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).
        String arch = "x86";      // system architecture
        String os = "Linux";          // operating system
        String vmm = "Xen";
        double time_zone = 10.0;        // time zone this resource located
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;		// the cost of using memory in this resource
        double costPerStorage = 0.1;	// the cost of using storage in this resource
        double costPerBw = 0.1;			// the cost of using bw in this resource
        
        LinkedList<Storage> storageList = new LinkedList<>();	//we are not adding SAN devices by now
        WorkflowDatacenter datacenter = null;

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

        // 5. Finally, we need to create a storage object.
        /**
         * The bandwidth within a data center in MB/s.
         */
        int maxTransferRate = 15;// the number comes from the futuregrid site, you can specify your bw

        try {
            // Here we set the bandwidth to be 15MB/s
            HarddriveStorage s1 = new HarddriveStorage(name, 1e12);
            s1.setMaxTransferRate(maxTransferRate);
            storageList.add(s1);
            datacenter = new WorkflowDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }
}
