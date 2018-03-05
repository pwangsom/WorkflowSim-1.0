package org.workflowsim.scheduling.mapreduce;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.util.JavaUtil;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.WorkflowSimTags;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;
import org.workflowsim.utils.VmRelationship;

public class MMHostNewMapReduceAlgorithm extends BaseSchedulingAlgorithm {

	protected int satisfyLocality = 0;
	protected int delayCount = 2;

	public MMHostNewMapReduceAlgorithm() {
		super();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {

		int size = getCloudletList().size();

		for (int i = 0; i < size; i++) {

			Cloudlet cloudlet = (Cloudlet) getCloudletList().get(i);
			Job job = (Job) cloudlet;

			int vmSize = getVmList().size();
			CondorVM closestVm = null;// (CondorVM)getVmList().get(0);
			int min_distance = 4;
			
			Task task = null;
			
			boolean isAvailableVm = false;

			for (int j = 0; j < vmSize; j++) {
				CondorVM vm = (CondorVM) getVmList().get(j);
				
				if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
					
					isAvailableVm = true;

					// For stage-in and reduce task
					if (job.getDepth() != 1) {
						closestVm = vm;
					} else {
						// For map task
						int distance = 4;
						
						if(!JavaUtil.isNull(job.getTaskList())){							
							task = job.getTaskList().get(0);
							
							distance = VmRelationship.getRelationshipType(vm.getId(), task.getDataStoredVmId())
									.distance();

							if (distance < min_distance) {
								min_distance = distance;
								closestVm = vm;
							}
							
						}
					}
				}
			}
			
            if(isAvailableVm && job.getDepth() == 1 && !JavaUtil.isNull(task) && min_distance > satisfyLocality && task.getDelayScheduleCount() < delayCount){
            	closestVm = null;
            	task.incrementDelayScheduleCount();
            } else {
            	
            	isAvailableVm = false;
            	
    			for (int j = 0; j < vmSize; j++) {
    				CondorVM vm = (CondorVM) getVmList().get(j);
    				
    				if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
    					
    					isAvailableVm = true;

    					// For stage-in and reduce task
    					if (job.getDepth() != 1) {
    						closestVm = vm;
    					} else {
    						// For map task
    						int distance = 4;
    						
    						if(!JavaUtil.isNull(job.getTaskList())){							
    							task = job.getTaskList().get(0);
    							
    							distance = VmRelationship.getRelationshipType(vm.getId(), task.getDataStoredVmId())
    									.distance();

    							if (distance < min_distance) {
    								min_distance = distance;
    								closestVm = vm;
    							}
    							
    						}
    					}
    				}
    			}
            	
            }

			if (closestVm != null) {
				closestVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
				cloudlet.setVmId(closestVm.getId());
				getScheduledList().add(cloudlet);	
			}
			
			// displayJobProperties(job);
		}
	}
	
}
