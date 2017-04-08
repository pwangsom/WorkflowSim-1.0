package org.cloudbus.cloudsim.util;

import java.text.DecimalFormat;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.workflowsim.FileItem;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.utils.JobScheduledResult;
import org.workflowsim.utils.Parameters.ClassType;
import org.workflowsim.utils.TaskScheduledResult;
import org.workflowsim.utils.VmRelationship;

public class DisplayUtil {
	
	public static void displayVmList(List<? extends Vm> vmList){
		Log.printLine("================ Display Utility: VM List ===================");
		for(Vm vm : vmList){
			Log.printLine("VM ID: " + vm.getId() + " at Host ID: " + vm.getHost().getId());
		}
	}
	
	public static void displayJobList(List<Job> jobList){
		Log.printLine("================ Display Utility: Job List ===================");
		for(Job job : jobList){
			Log.printLine("Job ID: " + job.getCloudletId() + " has the following tasks...");
			for(Task task : job.getTaskList()){
				Log.printLine("\tTask ID: " + task.getCloudletId() + " (depth " + task.getDepth() + ") requires the following files...");
				int i = 0;
				for(FileItem file : task.getFileList()){
					i++;
					Log.printLine("\t\t" + i + ". File name: " + file.getName() + ", size: " + file.getSize() + ", type: " + file.getType());
				}
			}
		}
	}
	
	private static String indent = "\t";
    private static DecimalFormat dft = new DecimalFormat("###.##");
	
	public static void displayExtendedCloudletList(List<? extends Cloudlet> list){
		
		if(!JavaUtil.isNull(list) && !list.isEmpty()){
			Log.printLine("================ Display Utility: <? extends Cloudlet>  List ===================");
	        Log.printLine("Instance Type" + indent
	        			+ "Job ID" + indent
	        			+ "User Task ID" + indent
	        			+ "STATUS" + indent + indent
	                	+ "Data center ID" + indent
	                	+ "VM ID" +  indent
	                	+ "Host ID" + indent + indent
	                	+ "Time" + indent + indent
	                	+ "Start Time" + indent
	                	+ "Finish Time" + indent
	                	+ "Task Type" + indent
	                	+ "Data Locality" + indent
	                	+ "VM ID" + indent
	                	+ "First Copy" + indent
	                	+ "Second Copy" + indent
	                	+ "Third Copy");
	        
			if(list.get(0) instanceof Job){
				for(Cloudlet cloudlet : list){
					displayLineOfJob((Job) cloudlet);
				}
			} else if(list.get(0) instanceof Task){
				
			} else if(list.get(0) instanceof Cloudlet){
				
			} else {
		        Log.printLine("Error: it is not the extension of Cloudlet");				
			}
		} else {
	        Log.printLine("Error: this list is empty.");
		}
		
	}
	
	private static void displayLineOfJob(Job instance){
		
		String taskId = "null";
		String taskStatus = "null";
		String locality = "null\t";
		
		int firstCopy = -1;
		int secondCopy = -1;
		int thirdCopy = -1;
		
		taskStatus = instance.getCloudletStatusString();		

		double executeTime = instance.getTaskFinishTime() - instance.getExecStartTime();
		double startTime = instance.getExecStartTime();
		double finishTime = instance.getTaskFinishTime();
		
		if(!JavaUtil.isNull(instance.getTaskList()) && !instance.getTaskList().isEmpty()){
			Task task = instance.getTaskList().get(0);
			
			if (instance.getClassType() == ClassType.STAGE_IN.value) {
				taskId = "Stage-in";
            } else {
    			taskId = String.valueOf(task.getCloudletId());            	
            }
			
			locality = VmRelationship.getRelationshipType(instance.getVmId(), task.getDataStoredVmId()).toString();
			
			firstCopy = task.getDataStoredVmIdByIndex(0);
			secondCopy = task.getDataStoredVmIdByIndex(1);
			thirdCopy = task.getDataStoredVmIdByIndex(2);
					
			executeTime = task.getTaskFinishTime() - task.getExecStartTime();
			startTime = task.getExecStartTime();
			finishTime = task.getTaskFinishTime();
		}
		
        String taskType = "Stage-in";
        
        if(instance.getDepth() == 1){
        	taskType = "Map Task";
        } else if(instance.getDepth() == 2){
        	taskType = "Reduce Task";
        }
		
		Log.printLine("Job" + indent + indent
					+ instance.getCloudletId() + indent
					+ taskId + indent + indent
				    + taskStatus + indent + indent
				    + instance.getResourceId() + indent + indent
				    + instance.getVmId() + indent
				    + "null" + indent + indent
            		+ dft.format(executeTime) + indent + indent 
            		+ dft.format(startTime) + indent + indent
                    + dft.format(finishTime) + indent + indent
                    + taskType + indent
                    + locality + indent
            		+ instance.getVmId() + indent
                    + firstCopy + indent + indent
                    + secondCopy + indent + indent
                    + thirdCopy
				    );
	}
	
    public static void displayJobScheduledResultListShort(JobScheduledResult jobScheduledResultList) {        
        DecimalFormat dft = new DecimalFormat("###.##");
        Log.printLine("Number of MapReduce Task:\t" + jobScheduledResultList.getNumOfMapTask());
        Log.printLine("VM Locality Ratio:\t\t" + jobScheduledResultList.getNumOfVmLocal() + "\t\t" + dft.format(jobScheduledResultList.getVmLocalRatio()));
        Log.printLine("Host Locality Ratio:\t\t" + jobScheduledResultList.getNumOfHostLocal() + "\t\t" +  dft.format(jobScheduledResultList.getHostLocalRatio()));
        Log.printLine("Rack Locality Ratio:\t\t" + jobScheduledResultList.getNumOfRackLocal() + "\t\t" + dft.format(jobScheduledResultList.getRackLocalRatio()));
        Log.printLine("Remote Locality Ratio:\t\t" + jobScheduledResultList.getNumOfRemoteLocal() + "\t\t" + dft.format(jobScheduledResultList.getRemoteLocalRatio()));
        

        Log.printLine("Start Time:\t\t\t" + dft.format(jobScheduledResultList.getMinStartTime()));
        Log.printLine("Finish Time:\t\t\t" + dft.format(jobScheduledResultList.getMaxFinishTime()));
        Log.printLine("Total Execution Time:\t\t" + dft.format(jobScheduledResultList.getTotalExecutetionTime()));
    }
	
    public static void displayJobScheduledResultList(JobScheduledResult jobScheduledResultList) {
        String indent = "\t";
        Log.printLine();
        Log.printLine("========== SCHEDULING ALGORITHM OUTPUT ==========");
        Log.printLine("Job ID" + indent
        			+ "User Task ID" + indent
        			+ "STATUS" + indent + indent
                	// + "Data center ID" + indent
                	+ "VM ID" +  indent
                	// + "Host ID" + indent
/*                	+ "No of Tasks" +  indent
                	+ "Schuduled Tasks" + indent
                	+ "Remain Tasks" + indent*/
                	+ "Time" + indent + indent
                	+ "Start Time" + indent
                	+ "Finish Time" + indent
                	+ "Task Type" + indent
                	+ "Data Locality" + indent + indent
                	// + "VM ID" + indent
/*                	+ "First Copy" + indent
                	+ "Second Copy" + indent
                	+ "Third Copy" + indent*/
                	+ "Delay Count"
                	);
        
        DecimalFormat dft = new DecimalFormat("###.##");
        
        for (JobScheduledResult resultItem : jobScheduledResultList.getJobScheduledResultList()) {
        	
        	String userTaskId = "null";
        	String hostId = null;
        	int delayCount = 0;
        	
        	if(!JavaUtil.isNull(resultItem.getHost())){
        		hostId = String.valueOf(resultItem.getHost().getId());
        	}
        	
        	int vmId = resultItem.getJob().getVmId();
        	
    		int firstCopy = resultItem.getJob().getDataStoredVmIdByIndex(0);
    		int secondCopy = resultItem.getJob().getDataStoredVmIdByIndex(1);
    		int thirdCopy = resultItem.getJob().getDataStoredVmIdByIndex(2); 		

    		double executeTime = resultItem.getJob().getTaskFinishTime() - resultItem.getJob().getExecStartTime();
    		double startTime = resultItem.getJob().getExecStartTime();
    		double finishTime = resultItem.getJob().getTaskFinishTime();
    		
    		if(!JavaUtil.isNull(resultItem.getTask())){
        		firstCopy = resultItem.getTask().getDataStoredVmIdByIndex(0);
        		secondCopy = resultItem.getTask().getDataStoredVmIdByIndex(1);
        		thirdCopy = resultItem.getTask().getDataStoredVmIdByIndex(2);   
        		
    			executeTime = resultItem.getTask().getTaskFinishTime() - resultItem.getTask().getExecStartTime();
    			startTime = resultItem.getTask().getExecStartTime();
    			finishTime = resultItem.getTask().getTaskFinishTime();
    			
    			delayCount = resultItem.getTask().getDelayScheduleCount();
    		}

            if (resultItem.getJob().getClassType() == ClassType.STAGE_IN.value) {
            	userTaskId = "0";
            	
        		executeTime = resultItem.getJob().getActualCPUTime();
        		startTime = resultItem.getJob().getExecStartTime();
        		finishTime = resultItem.getJob().getFinishTime();
        		
            } else {
            	userTaskId = String.valueOf(resultItem.getTask().getCloudletId());
            }
            
            String taskType = "Stage-in";
            
            if(resultItem.getJob().getDepth() == 1){
            	taskType = "Map Task";
            } else if(resultItem.getJob().getDepth() == 2){
            	taskType = "Reduce Task";
            }
            
            Log.printLine(resultItem.getJob().getCloudletId() + indent
            			+ userTaskId + indent + indent
            			+ resultItem.getJob().getCloudletStatusString() + indent + indent
            			// + resultItem.getJob().getResourceId() + indent + indent
	            		+ vmId + indent
	            		// + hostId + indent
/*	            		+ resultItem.getNumOfCloudlet() + indent + indent
	            		+ resultItem.getSuccessSchedule() + indent + indent
	            		+ resultItem.getRemainSchedule() + indent + indent*/
	            		+ dft.format(executeTime) + indent + indent 
	            		+ dft.format(startTime) + indent + indent
	                    + dft.format(finishTime) + indent + indent
	                    + taskType + indent 
	                    + resultItem.getLocalityType() + indent + indent
	            		//+ vmId + indent
/*	                    + firstCopy + indent + indent
	                    + secondCopy + indent + indent
	                    + thirdCopy + indent*/
	                    + delayCount
	                    );
        }
        
        Log.printLine();
        Log.printLine("Number of MapReduce Task:\t" + jobScheduledResultList.getNumOfMapTask());
        Log.printLine("VM Locality Ratio:\t\t" + jobScheduledResultList.getNumOfVmLocal() + "\t\t" + dft.format(jobScheduledResultList.getVmLocalRatio()));
        Log.printLine("Host Locality Ratio:\t\t" + jobScheduledResultList.getNumOfHostLocal() + "\t\t" +  dft.format(jobScheduledResultList.getHostLocalRatio()));
        Log.printLine("Rack Locality Ratio:\t\t" + jobScheduledResultList.getNumOfRackLocal() + "\t\t" + dft.format(jobScheduledResultList.getRackLocalRatio()));
        Log.printLine("Remote Locality Ratio:\t\t" + jobScheduledResultList.getNumOfRemoteLocal() + "\t\t" + dft.format(jobScheduledResultList.getRemoteLocalRatio()));
        

        Log.printLine("Start Time:\t\t\t" + dft.format(jobScheduledResultList.getMinStartTime()));
        Log.printLine("Finish Time:\t\t\t" + dft.format(jobScheduledResultList.getMaxFinishTime()));
        Log.printLine("Total Execution Time:\t\t" + dft.format(jobScheduledResultList.getTotalExecutetionTime()));
        
        Log.printLine("========== SCHEDULING ALGORITHM OUTPUT ==========");
    }
	
    public static void displayTaskScheduledResultList(List<TaskScheduledResult> taskScheduledResultList, List<Job> jobList) {
        String indent = "\t";
        Log.printLine();
        Log.printLine("========== SCHEDULING ALGORITHM OUTPUT ==========");
        Log.printLine("Job ID" + indent
        			+ "User Task ID" + indent
        			+ "STATUS" + indent + indent
                	+ "Data center ID" + indent
                	+ "VM ID" +  indent
                	+ "Host ID" + indent + indent
                	+ "No of Tasks" +  indent
                	+ "Schuduled Tasks" + indent
                	+ "Remain Tasks" + indent
                	+ "Time" + indent + indent
                	+ "Start Time" + indent
                	+ "Finish Time" + indent
                	+ "Task Type" + indent
                	+ "Data Locality" + indent + indent
                	+ "VM ID" + indent
                	+ "First Copy" + indent
                	+ "Second Copy" + indent
                	+ "Third Copy");
        
        DecimalFormat dft = new DecimalFormat("###.##");
        
        for (TaskScheduledResult task : taskScheduledResultList) {
        	
        	Job job = getJobByCloudletId(task.getTask().getCloudletId(), jobList);
        	
            Log.print(task.getTask().getCloudletId() + indent);

            if (job.getClassType() == ClassType.STAGE_IN.value) {
                Log.print("Stage-in" + indent);
            }
            for (Task t : job.getTaskList()) {
                Log.print(t.getCloudletId() + "," + indent + indent);
            }
            
            if (task.getTask().getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");
            } else if (task.getTask().getCloudletStatus() == Cloudlet.FAILED) {
                Log.print("FAILED");
            }

            Log.print(indent + indent);
            
            String taskType = "Stage-in";
            
            if(job.getDepth() == 1){
            	taskType = "Map Task";
            } else if(job.getDepth() == 2){
            	taskType = "Reduce Task";
            }
            
            Log.printLine(task.getTask().getResourceId() + indent + indent
            		+ task.getVm().getId() + indent
            		+ task.getHost().getId() + indent + indent
            		+ task.getNumOfCloudlet() + indent + indent
            		+ task.getSuccessSchedule() + indent + indent
            		+ task.getRemainSchedule() + indent + indent
            		+ dft.format(task.getTask().getActualCPUTime()) + indent + indent 
            		+ dft.format(task.getTask().getExecStartTime()) + indent + indent
                    + dft.format(task.getTask().getFinishTime()) + indent + indent
                    + taskType + indent 
                    + task.getLocalityType() + indent + indent
            		+ task.getVm().getId() + indent
                    + task.getTask().getDataStoredVmId()[0] + indent + indent
                    + task.getTask().getDataStoredVmId()[1] + indent + indent
                    + task.getTask().getDataStoredVmId()[2]);
        }
        
        Log.printLine("========== SCHEDULING ALGORITHM OUTPUT ==========");
    }
       
    public static void displayTimeOfJob(List<Job> jobList){
    	
    	
    	for(Job job : jobList){
    		
    		Task task = new Task(0, 0);
    		String taskId = "0";
    		
    		if(!JavaUtil.isNull(job.getTaskList()) && !job.getTaskList().isEmpty()){
    			task = job.getTaskList().get(0);
    			
    			if (job.getClassType() == ClassType.STAGE_IN.value) {
    				taskId = "99";
                } else {
        			taskId = String.valueOf(task.getCloudletId());            	
                }
    		}
    		
    		Log.printLine("[JobID: " + job.getCloudletId() + "],\t"
    					+ "[TaskID: " + taskId + "],\t"
    	    			+ "[Depth: " + job.getDepth() + "],\t"
    	    	    	+ "[JobClLenght: " + job.getCloudletLength() + "],\t"
    	    	    	+ "[JobLocalLenght: " + job.getLocalityPenaltyLength() + "],\t\t"
    	    	    	+ "[JobActualCPUTime: " + dft.format(job.getActualCPUTime()) + "],\t"
    	    	    	+ "[JobExecStartTime: " + dft.format(job.getExecStartTime()) + "],\t"
    	    	    	+ "[JobFinishTime: " + dft.format(job.getFinishTime()) + "],\t"
    	    	    	+ "[JobSubmissionTime: " + dft.format(job.getSubmissionTime()) + "],\t"
    	    	    	+ "[TaskClLenght: " + task.getCloudletLength() + "],\t"
    	    	    	+ "[TaskLocalLenght: " + task.getLocalityPenaltyLength() + "],\t\t"
    	    	    	+ "[TaskActualCPUTime: " + dft.format(task.getActualCPUTime()) + "],\t"
    	    	    	+ "[TaskExecStartTime: " + dft.format(task.getExecStartTime()) + "],\t"
    	    	    	+ "[TaskTaskFinishTime: " + dft.format(task.getTaskFinishTime()) + "]"
    		);
    	}
    }
    
    private static Job getJobByCloudletId(int cloudletId, List<Job> jobList){
    	Job result = null;
    	for(Job job : jobList){
    		if(job.getCloudletId() == cloudletId){
    			result = job;
    			break;
    		}
    	}
    	return result;
    }
    
    public static void displayJobProperties(Job job){
    	int taskId = -1;
		String locality = "null";
		
		int firstCopy = -1;
		int secondCopy = -1;
		int thirdCopy = -1;
		
		double executeTime = job.getTaskFinishTime() - job.getExecStartTime();
		double startTime = job.getExecStartTime();
		double finishTime = job.getTaskFinishTime();
		
		long jobLength = job.getCloudletLength();
		long penalty = job.getLocalityPenaltyLength();
		
		int delay = 0;
    	
    	if(!JavaUtil.isNull(job.getTaskList()) && !job.getTaskList().isEmpty()){
    		Task task = job.getTaskList().get(0);

            if (job.getClassType() == ClassType.STAGE_IN.value) {
            	taskId = 0;
            } else {
            	taskId = task.getCloudletId();
            }
            
            if(job.getVmId() >-1 && job.getVmId() < 8) {
            	locality = VmRelationship.getRelationshipType(job.getVmId(), task.getDataStoredVmId()).toString();
            }
            
			firstCopy = task.getDataStoredVmIdByIndex(0);
			secondCopy = task.getDataStoredVmIdByIndex(1);
			thirdCopy = task.getDataStoredVmIdByIndex(2);       
			
			executeTime = task.getTaskFinishTime() - task.getExecStartTime();
			startTime = task.getExecStartTime();
			finishTime = task.getTaskFinishTime();
			
			jobLength = task.getCloudletLength();
			penalty = task.getLocalityPenaltyLength();
			
			delay = task.getDelayScheduleCount();
    	}
    	
		Log.printLine("Job_ID: " + job.getCloudletId()
					+ ", Task_ID: " + taskId
					+ ", Depth: " + job.getDepth()
				    + ", VM_ID: " + job.getVmId()
				    + ", Length: " + jobLength
				    + ", Penalty: " + penalty 
	        		+ ", Time: " + dft.format(executeTime)
	        		+ ", Start: " + dft.format(startTime)
	                + ", Finish: " + dft.format(finishTime)
	                + ", Locality: " + locality
	                + ", 1st: " + firstCopy
	                + ", 2nd: " + secondCopy
	                + ", 3rd: " + thirdCopy
	                + ", Delay: " + delay
				    );
    }

}
