package org.workflowsim.utils;

import org.cloudbus.cloudsim.Host;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;

public class TaskScheduledResult {
	private Task task;
	private CondorVM vm;
	private Host host;
	private int numOfCloudlet;
	private int successSchedule;
	private int remainSchedule;

	private LocalityType localityType;

	public TaskScheduledResult(Task task, CondorVM vm, Host host, int numOfCloudlet, int successSchedule) {
		this.task = task;
		this.vm = vm;
		this.host = host;
		this.numOfCloudlet = numOfCloudlet;
		this.successSchedule = successSchedule;
		this.remainSchedule = this.numOfCloudlet - this.successSchedule;
		setLocalityType();
	}

	private void setLocalityType() {

		int[] fileLocation = this.task.getDataStoredVmId();
		int relation = 3;

		for (int i = 0; i < fileLocation.length; i++) {
			if (fileLocation[i] > -1 && fileLocation[i] < 8) {
				int distance = VmRelationship.getRelationshipType(vm.getId(), fileLocation[i]).distance();
				if (distance < relation) {
					relation = distance;
				}
			} else {
				relation = 4;
			}
		}

		this.localityType = LocalityType.fromDistance(relation);
	}

	public Task getTask() {
		return task;
	}

	public void setTask(Task task) {
		this.task = task;
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
}
