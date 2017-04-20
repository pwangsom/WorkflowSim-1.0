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
package org.workflowsim.scheduling.mapreduce;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.util.JavaUtil;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.WorkflowSimTags;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;
import org.workflowsim.utils.VmRelationship;

/**
 * Data aware algorithm. Schedule a job to a vm that has most input data it
 * requires. It only works for a local environment.
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class DelayMapReduceAlgorithm extends BaseSchedulingAlgorithm {
	
	private int satisfyLocality = 0;
	private int delayCount = 4;

	public DelayMapReduceAlgorithm() {
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
