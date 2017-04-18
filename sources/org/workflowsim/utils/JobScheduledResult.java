package org.workflowsim.utils;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.util.DisplayUtil;
import org.cloudbus.cloudsim.util.JavaUtil;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.Task;

public class JobScheduledResult {
	
	private Job job = null;
	private Task task = null;
	
	private CondorVM vm = null;
	private Host host = null;
	
	private int numOfCloudlet = 0;
	private int successSchedule = 0;
	private int remainSchedule = 0;
	
	private double numOfTask = 0;
	private double numOfVmLocal = 0;
	private double numOfHostLocal = 0;
	private double numOfRackLocal = 0;
	private double numOfRemoteLocal = 0;
	
	private double minStartTime = 1000000000;
	private double maxFinishTime = 0;

	private LocalityType localityType;
	
	private List<JobScheduledResult> jobScheduledResultList;
	
	public JobScheduledResult() {
		this.jobScheduledResultList = new ArrayList<JobScheduledResult>();
	}
	
	public JobScheduledResult(Job job, CondorVM vm, Host host, int numOfCloudlet, int successSchedule) {
		this.job = job;
		this.vm = vm;
		this.host = host;
		this.numOfCloudlet = numOfCloudlet;
		this.successSchedule = successSchedule;
		this.remainSchedule = this.numOfCloudlet - this.successSchedule;
		setTask();
		setLocalityType();
	}
	
	public JobScheduledResult(Job job, CondorVM vm, Host host) {
		this.job = job;
		this.vm = vm;
		this.host = host;
		setTask();
		setLocalityType();
	}	
	
	public JobScheduledResult(Job job) {
		this.job = job;
		setTask();
		setLocalityType();
	}
	
	public void add(JobScheduledResult jobResult){
		if(JavaUtil.isNull(this.jobScheduledResultList)) this.jobScheduledResultList = new ArrayList<JobScheduledResult>();
		this.jobScheduledResultList.add(jobResult);
		computeLocalityRatio(jobResult);
		recordMinMaxTime(jobResult);
	}
	
	public List<JobScheduledResult> getJobScheduledResultList(){
		return this.jobScheduledResultList;
	}
	
	private void computeLocalityRatio(JobScheduledResult jobResult) {
		if (jobResult.getJob().getDepth() == 1 || jobResult.getJob().getDepth() == 2) {
			
			DisplayUtil.displayJobProperties(jobResult.getJob());
			
			numOfTask++;
			switch (jobResult.getLocalityType()) {
			case VM_LOCALITY:
				numOfVmLocal++;
				break;
			case HOST_LOCALITY:
				numOfHostLocal++;
				break;
			case RACK_LOCALITY:
				numOfRackLocal++;
				break;
			case REMOTE_LOCALITY:
				numOfRemoteLocal++;
				break;
			default:
				break;
			}
		}
	}
	
	private void recordMinMaxTime(JobScheduledResult jobResult){
		if (jobResult.getJob().getDepth() == 1 || jobResult.getJob().getDepth() == 2) {
			if(!JavaUtil.isNull(jobResult.task)){
				if(jobResult.task.getExecStartTime() < this.minStartTime) this.minStartTime = jobResult.task.getExecStartTime();
				if(jobResult.task.getTaskFinishTime() > this.maxFinishTime) this.maxFinishTime = jobResult.task.getTaskFinishTime();
			}
		}
	}
	
	private void setTask(){
		if(!JavaUtil.isNull(this.job.getTaskList()) && !this.job.getTaskList().isEmpty()){
			this.task = this.job.getTaskList().get(0);
		} else {
			this.task = null;
		}
	}
	
	private void setLocalityType() {
		int relation = 4;
		int vmId = this.job.getVmId();

		if(!JavaUtil.isNull(this.task) && this.job.getDepth() == 1){			
			int[] fileLocation = this.task.getDataStoredVmId();

			for (int i = 0; i < fileLocation.length; i++) {
				if (fileLocation[i] > -1 && fileLocation[i] < 8) {
					int distance = VmRelationship.getRelationshipType(vmId, fileLocation[i]).distance();
					if (distance < relation) {
						relation = distance;
					}
				} 
			}
		} else if(this.job.getDepth() == 2){
			relation = 3;
		}

		this.localityType = LocalityType.fromDistance(relation);
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}
	
	public Task getTask() {
		return task;
	}


	public CondorVM getVm() {
		return vm;
	}

	public void setVm(CondorVM vm) {
		this.vm = vm;
	}

	public Host getHost() {
		return host;
	}

	public void setHost(Host host) {
		this.host = host;
	}

	public int getNumOfCloudlet() {
		return numOfCloudlet;
	}

	public void setNumOfCloudlet(int numOfCloudlet) {
		this.numOfCloudlet = numOfCloudlet;
	}

	public int getSuccessSchedule() {
		return successSchedule;
	}

	public void setSuccessSchedule(int successSchedule) {
		this.successSchedule = successSchedule;
	}

	public int getRemainSchedule() {
		return remainSchedule;
	}

	public void setRemainSchedule(int remainSchedule) {
		this.remainSchedule = remainSchedule;
	}

	public LocalityType getLocalityType() {
		return localityType;
	}

	public void setLocalityType(LocalityType localityType) {
		this.localityType = localityType;
	}

	public double getMinStartTime() {
		return maxFinishTime < minStartTime?0:minStartTime;
	}

	public double getMaxFinishTime() {
		return maxFinishTime < minStartTime?0:maxFinishTime;
	}

	public double getNumOfTask() {
		return numOfTask;
	}

	public void setNumOfTask(double numOfTask) {
		this.numOfTask = numOfTask;
	}

	public double getNumOfVmLocal() {
		return numOfVmLocal;
	}

	public void setNumOfVmLocal(double numOfVmLocal) {
		this.numOfVmLocal = numOfVmLocal;
	}

	public double getNumOfHostLocal() {
		return numOfHostLocal;
	}

	public void setNumOfHostLocal(double numOfHostLocal) {
		this.numOfHostLocal = numOfHostLocal;
	}

	public double getNumOfRackLocal() {
		return numOfRackLocal;
	}

	public void setNumOfRackLocal(double numOfRackLocal) {
		this.numOfRackLocal = numOfRackLocal;
	}

	public double getNumOfRemoteLocal() {
		return numOfRemoteLocal;
	}

	public void setNumOfRemoteLocal(double numOfRemoteLocal) {
		this.numOfRemoteLocal = numOfRemoteLocal;
	}
	
	public double getVmLocalRatio(){
		return (this.numOfVmLocal/this.numOfTask)*100;
	}
	
	public double getHostLocalRatio(){
		return (this.numOfHostLocal/this.numOfTask)*100;
	}
	
	public double getRackLocalRatio(){
		return (this.numOfRackLocal/this.numOfTask)*100;
	}
	
	public double getRemoteLocalRatio(){
		return (this.numOfRemoteLocal/this.numOfTask)*100;
	}
	
	public double getTotalExecutetionTime(){
		if(this.maxFinishTime < this.minStartTime){
			return 0;
		} else {
			return this.maxFinishTime - this.minStartTime;
		}
	}
}
