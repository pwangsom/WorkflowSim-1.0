package org.workflowsim.utils;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.Job;
import org.workflowsim.Task;

/**
 * 
 * @author Dell
 * Distributed File System
 *
 */

public class DFSCatalog {

/*
	private static final int VMS = 8;
	
	private static final List<Integer> RACK_0 = new ArrayList<Integer>(Arrays.asList(0, 1, 4, 5));
	private static final List<Integer> RACK_1 = new ArrayList<Integer>(Arrays.asList(2, 3, 6, 7));
	
	public static boolean initialDFSCatalog(List<? extends Cloudlet> cloudletList){
		
		try {
			List<Job> mapTaskList = getMapTaskList(cloudletList);

			int size = mapTaskList.size();
			
			int rackCopyOfRack0Index = 0;
			int remoteCopyOfRack0Index = 0;
			
			int rackCopyOfRack1Index = 0;
			int remoteCopyOfRack1Index = 0;
			
			for(int i = 0; i < size; i++){
				
				int targetVmId = i%VMS;				
				Job job = mapTaskList.get(i);				
				
				for(Task task : job.getTaskList()){
					if(task.getDepth() == 1){
						if(RACK_0.contains(targetVmId)){
							if(RACK_0.get(rackCopyOfRack0Index%4) == targetVmId){
								rackCopyOfRack0Index++;
							}
							
							task.setDataStoredVmIdByIndex(0, targetVmId);
							task.setDataStoredVmIdByIndex(1, RACK_0.get(rackCopyOfRack0Index%4));
							task.setDataStoredVmIdByIndex(2, RACK_1.get(remoteCopyOfRack0Index%4));
							
							rackCopyOfRack0Index++;
							remoteCopyOfRack0Index++;
						} else {
							if(RACK_1.get(rackCopyOfRack1Index%4) == targetVmId){
								rackCopyOfRack1Index++;
							}					

							task.setDataStoredVmIdByIndex(0, targetVmId);
							task.setDataStoredVmIdByIndex(1, RACK_1.get(rackCopyOfRack1Index%4));
							task.setDataStoredVmIdByIndex(2, RACK_0.get(remoteCopyOfRack1Index%4));
							
							rackCopyOfRack1Index++;
							remoteCopyOfRack1Index++;
						}
						break;
					}
				}
			}
			
			return true;
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return false;
		}
	}
*/
	
	// private static final int VMS = 16;
	
	public static boolean initialDFSCatalog(List<? extends Cloudlet> cloudletList){
		
		int copy1 = 4;
		int copy2 = 10;
		int copy3 = 14;
		
		try {
			List<Job> mapTaskList = getMapTaskList(cloudletList);

			int size = mapTaskList.size();
			
			for(int i = 0; i < size; i++){
				
				int index1 = ((i%MapReduceParameter.NO_VMS)+copy1)%MapReduceParameter.NO_VMS;				
				Job job = mapTaskList.get(i);				
				
				for(Task task : job.getTaskList()){
					if(task.getDepth() == 1){
						
						int index2 = (index1+copy2)%MapReduceParameter.NO_VMS;
						int index3 = (index1+copy3)%MapReduceParameter.NO_VMS;
						
						task.setDataStoredVmIdByIndex(0, index1);
						task.setDataStoredVmIdByIndex(1, index2);
						task.setDataStoredVmIdByIndex(2, index3);
						
					}
				}
			}
			
			return true;
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return false;
		}
	}
	
	public static void printDFSCatalog(List<? extends Cloudlet> cloudletList){
		
		List<Job> mapTaskList = getMapTaskList(cloudletList);		

        String indent = "\t";
		Log.printLine("================ Print DFS Catalog ===================");
        Log.printLine("User Task ID" + indent
    				+ "First Copy" + indent
    				+ "Second Copy" + indent
    				+ "Third Copy");

        try {
    		for(Job job : mapTaskList){			
    			for(Task task : job.getTaskList()){
    				if(task.getDepth() == 1){
    					Log.printLine(task.getCloudletId() + indent + indent
    							+ task.getDataStoredVmIdByIndex(0) + indent + indent
    							+ task.getDataStoredVmIdByIndex(1) + indent + indent
    							+ task.getDataStoredVmIdByIndex(2));
    					break;
    				}
    			}
    		}
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	private static List<Job> getMapTaskList(List<? extends Cloudlet> cloudletList){
		List<Job> result = new ArrayList<Job>();
		
		for(Cloudlet cloudlet : cloudletList){
			Job job = (Job) cloudlet;
			
			for(Task task : job.getTaskList()){
				if(task.getDepth() == 1){
					result.add(job);
					break;
				}
			}
		}
		
		return result;
	}
}
