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
package org.workflowsim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;
import org.workflowsim.scheduling.DataAwareSchedulingAlgorithm;
import org.workflowsim.scheduling.FCFSSchedulingAlgorithm;
import org.workflowsim.scheduling.MCTSchedulingAlgorithm;
import org.workflowsim.scheduling.MaxMinSchedulingAlgorithm;
import org.workflowsim.scheduling.MinMinSchedulingAlgorithm;
import org.workflowsim.scheduling.RoundRobinSchedulingAlgorithm;
import org.workflowsim.scheduling.StaticSchedulingAlgorithm;
import org.workflowsim.scheduling.mapreduce.DLHost4MapReduceAlgorithm;
import org.workflowsim.scheduling.mapreduce.DLVm4MapReduceAlgorithm;
import org.workflowsim.scheduling.mapreduce.DataAwareMapReduceAlgorithm;
import org.workflowsim.scheduling.mapreduce.DelayMapReduceAlgorithm;
import org.workflowsim.scheduling.mapreduce.FCFSMapReduceAlgorithm;
import org.workflowsim.scheduling.mapreduce.MMHost2MapReduceAlgorithm;
import org.workflowsim.scheduling.mapreduce.MMHostNewMapReduceAlgorithm;
import org.workflowsim.scheduling.mapreduce.MMVm2MapReduceAlgorithm;
import org.workflowsim.utils.JobScheduledResult;
import org.workflowsim.utils.LocalityType;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.SchedulingAlgorithm;
import org.workflowsim.utils.TaskScheduledResult;
import org.workflowsim.utils.VmRelationship;

/**
 * WorkflowScheduler represents a algorithm acting on behalf of a user. It hides
 * VM management, as vm creation, sumbission of jobs to this VMs and destruction
 * of VMs. It picks up a scheduling algorithm based on the configuration
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class WorkflowScheduler extends DatacenterBroker {

    /**
     * The workflow engine id associated with this workflow algorithm.
     */
    private int workflowEngineId;

    /**
     * Created a new WorkflowScheduler object.
     *
     * @param name name to be associated with this entity (as required by
     * Sim_entity class from simjava package)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public WorkflowScheduler(String name) throws Exception {
        super(name);
    }

    /**
     * Binds this scheduler to a datacenter
     *
     * @param datacenterId data center id
     */
    public void bindSchedulerDatacenter(int datacenterId) {
        if (datacenterId <= 0) {
            Log.printLine("Error in data center id");
            return;
        }
        this.datacenterIdsList.add(datacenterId);
    }

    /**
     * Sets the workflow engine id
     *
     * @param workflowEngineId the workflow engine id
     */
    public void setWorkflowEngineId(int workflowEngineId) {
        this.workflowEngineId = workflowEngineId;
    }

    /**
     * Process an event
     *
     * @param ev a simEvent obj
     */
    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            // Resource characteristics request
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            // Resource characteristics answer
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                processResourceCharacteristics(ev);
                break;
            // VM Creation answer
            case CloudSimTags.VM_CREATE_ACK:
                processVmCreate(ev);
                break;
            // A finished cloudlet returned
            case WorkflowSimTags.CLOUDLET_CHECK:
                processCloudletReturn(ev);
                break;
            case CloudSimTags.CLOUDLET_RETURN:
                processCloudletReturn(ev);
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            case CloudSimTags.CLOUDLET_SUBMIT:
                processCloudletSubmit(ev);
                break;
            case WorkflowSimTags.CLOUDLET_UPDATE:
                processCloudletUpdate(ev);
                break;
            default:
                processOtherEvent(ev);
                break;
        }
    }

    /**
     * Switch between multiple schedulers. Based on algorithm.method
     *
     * @param name the SchedulingAlgorithm name
     * @return the algorithm that extends BaseSchedulingAlgorithm
     */
    private BaseSchedulingAlgorithm getScheduler(SchedulingAlgorithm name) {
        BaseSchedulingAlgorithm algorithm;

        // choose which algorithm to use. Make sure you have add related enum in
        //Parameters.java
        switch (name) {
            //by default it is Static
            case FCFS:
                algorithm = new FCFSSchedulingAlgorithm();
                break;
            case MINMIN:
                algorithm = new MinMinSchedulingAlgorithm();
                break;
            case MAXMIN:
                algorithm = new MaxMinSchedulingAlgorithm();
                break;
            case MCT:
                algorithm = new MCTSchedulingAlgorithm();
                break;
            case DATA:
                algorithm = new DataAwareSchedulingAlgorithm();
                break;
            case STATIC:
                algorithm = new StaticSchedulingAlgorithm();
                break;
            case ROUNDROBIN:
                algorithm = new RoundRobinSchedulingAlgorithm();
                break;
            case FCFS_MR:
            	algorithm = new FCFSMapReduceAlgorithm();
            	break;
            case DATA_MR:
            	algorithm = new DataAwareMapReduceAlgorithm();
            	break;
            case MM_VM_2:
            	algorithm = new MMVm2MapReduceAlgorithm();
            	break;
            case MM_HO_2:
            	algorithm = new MMHost2MapReduceAlgorithm();
            	break;
            case MM_HO_NEW:
            	algorithm = new MMHostNewMapReduceAlgorithm();
            	break;
            case DELAY_MR:
            	algorithm = new DelayMapReduceAlgorithm();
            	break;
            case DL_VM_4:
            	algorithm = new DLVm4MapReduceAlgorithm();
            	break;
            case DL_HO_4:
            	algorithm = new DLHost4MapReduceAlgorithm();
            	break;
            default:
                algorithm = new StaticSchedulingAlgorithm();
                break;

        }
        return algorithm;
    }

    /**
     * Process the ack received due to a request for VM creation.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     */
    @Override
    protected void processVmCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            getVmsToDatacentersMap().put(vmId, datacenterId);
            /**
             * Fix a bug of cloudsim Don't add a null to getVmsCreatedList()
             * June 15, 2013
             */
            if (VmList.getById(getVmList(), vmId) != null) {
                getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
                Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
                        + " has been created in Datacenter #" + datacenterId + ", Host #"
                        + VmList.getById(getVmsCreatedList(), vmId).getHost().getId());
            }
        } else {
            Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId
                    + " failed in Datacenter #" + datacenterId);
        }

        incrementVmsAcks();

        // all the requested VMs have been created
        if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
            submitCloudlets();
        } else {
            // all the acks received, but some VMs were not created
            if (getVmsRequested() == getVmsAcks()) {
                // find id of the next datacenter that has not been tried
                for (int nextDatacenterId : getDatacenterIdsList()) {
                    if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) {
                        createVmsInDatacenter(nextDatacenterId);
                        return;
                    }
                }

                // all datacenters already queried
                if (getVmsCreatedList().size() > 0) { // if some vm were created
                    submitCloudlets();
                } else { // no vms created. abort
                    Log.printLine(CloudSim.clock() + ": " + getName()
                            + ": none of the required VMs could be created. Aborting");
                    finishExecution();
                }
            }
        }
    }

    /**
     * Update a cloudlet (job)
     *
     * @param ev a simEvent object
     */
    protected void processCloudletUpdate(SimEvent ev) {

    	// Peerasak
        // Log.printLine();
        // Log.printLine(getName() + ": processCloudletUpdate(SimEvent ev) by WorkflowSimTags.CLOUDLET_UPDATE)");
        
        // int preSize = getCloudletList().size();

        BaseSchedulingAlgorithm scheduler = getScheduler(Parameters.getSchedulingAlgorithm());
        scheduler.setCloudletList(getCloudletList());
        scheduler.setVmList(getVmsCreatedList());

        try {
            scheduler.run();
        } catch (Exception e) {
            Log.printLine("Error in configuring scheduler_method");
            e.printStackTrace();
        }        

        @SuppressWarnings("unchecked")
		List<Cloudlet> scheduledList = scheduler.getScheduledList();
        for (Cloudlet cloudlet : scheduledList) {
            int vmId = cloudlet.getVmId();
            double delay = 0.0;
            if (Parameters.getOverheadParams().getQueueDelay() != null) {
                delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
            }
            

            Job job = (Job) cloudlet;
            
/*            CondorVM vm = VmList.getById(getVmList(), vmId);
            Host host = vm.getHost();
            
            int taskId = 0;
            
            if(!JavaUtil.isNull(job.getTaskList()) && !job.getTaskList().isEmpty()){
            	taskId = job.getTaskList().get(0).getCloudletId();
            }*/
            
            // Log.printLine(CloudSim.clock() + ": " + getName() + ": Sending Job ID: " + job.getCloudletId() + " : User task ID: " + taskId + " to VM ID: " + vmId + " of Host ID: " + host.getId());
            
            // cloudletScheduleResultList.add(new CloudletScheduleResult(cloudlet, vm, host, getCloudletList().size(), scheduledList.size()));
            // taskScheduledResultList.add(new TaskScheduledResult((Task) cloudlet, vm, host, getCloudletList().size(), scheduledList.size()));
            // jobScheduledResultList.add(new JobScheduledResult((Job) cloudlet, vm, host, getCloudletList().size(), scheduledList.size()));
            
            updateLocalityAfterScheduled(job);
            
            scheduledJob.add(job.getCloudletId());

        	// Log.printLine("scheduledJobList size " + scheduledJob.size() + "; CloudletId " + job.getCloudletId() + " has been added");
            
            schedule(getVmsToDatacentersMap().get(vmId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
        }                
        getCloudletList().removeAll(scheduledList);
        getCloudletSubmittedList().addAll(scheduledList);
        cloudletsSubmitted += scheduledList.size();
        
/*
        Log.printLine("processCloudletUpdate(SimEvent ev): Incoming Cloudlet List Size: " + preSize + "; Scheduled List Size: " + scheduledList.size()
        			+ "; Remaining Cloudlet List Size: " + getCloudletList().size() + "; cloudletsSubmitted: " + cloudletsSubmitted);
        */
    }
    
    private void updateLocalityAfterScheduled(Job job){
        for (Task task : job.getTaskList()) {
        	
        	if(job.getDepth() == 2){
            	task.setLocalityType(LocalityType.REMOTE_LOCALITY);          		
        	} else {
            	task.setLocalityType(VmRelationship.getRelationshipType(job.getVmId(), task.getDataStoredVmId()));        		
        	}
        	
        	task.setLocalityPenaltyLength(task.getLocalityType().penaltyLenght());
        	
        	job.setLocalityType(task.getLocalityType());
        	job.setLocalityPenaltyLength(task.getLocalityPenaltyLength());
        	// DisplayUtil.displayJobProperties(job);
        	job.setCloudletLength(job.getCloudletLength() + job.getLocalityPenaltyLength());
        	// DisplayUtil.displayJobProperties(job);
        }
    }
    
    private List<Integer> scheduledJob = new ArrayList<Integer>();
    
    public List<Integer> getScheduledJob(){
    	return scheduledJob;
    }

    /**
     * Process a cloudlet (job) return event.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    @Override
    protected void processCloudletReturn(SimEvent ev) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        Job job = (Job) cloudlet;

        /**
         * Generate a failure if failure rate is not zeros.
         */
        FailureGenerator.generate(job);

        getCloudletReceivedList().add(cloudlet);
        getCloudletSubmittedList().remove(cloudlet);

        CondorVM vm = (CondorVM) getVmsCreatedList().get(cloudlet.getVmId());
        //so that this resource is released
        vm.setState(WorkflowSimTags.VM_STATUS_IDLE);

        double delay = 0.0;
        if (Parameters.getOverheadParams().getPostDelay() != null) {
            delay = Parameters.getOverheadParams().getPostDelay(job);
        }
        schedule(this.workflowEngineId, delay, CloudSimTags.CLOUDLET_RETURN, cloudlet);

        cloudletsSubmitted--;
        //not really update right now, should wait 1 s until many jobs have returned
        schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);

    }

    /**
     * Start this entity (WorkflowScheduler)
     */
    @Override
    public void startEntity() {
        Log.printLine(getName() + " is starting...");
        // this resource should register to regional GIS.
        // However, if not specified, then register to system GIS (the
        // default CloudInformationService) entity.
        //int gisID = CloudSim.getEntityId(regionalCisName);
        int gisID = -1;
        if (gisID == -1) {
            gisID = CloudSim.getCloudInfoServiceEntityId();
        }

        // send the registration to GIS
        sendNow(gisID, CloudSimTags.REGISTER_RESOURCE, getId());
    }

    /**
     * Terminate this entity (WorkflowScheduler)
     */
    @Override
    public void shutdownEntity() {
        clearDatacenters();
        Log.printLine(getName() + " is shutting down...");
    }

    /**
     * Submit cloudlets (jobs) to the created VMs. Scheduling is here
     */
    @Override
    protected void submitCloudlets() {	    	
        sendNow(this.workflowEngineId, CloudSimTags.CLOUDLET_SUBMIT, getVmList());
    }
    /**
     * A trick here. Assure that we just submit it once
     */
    private boolean processCloudletSubmitHasShown = false;

    /**
     * Submits cloudlet (job) list
     *
     * @param ev a simEvent object
     */
    protected void processCloudletSubmit(SimEvent ev) {
        List<Job> list = (List<Job>) ev.getData();
        getCloudletList().addAll(list);
        
        sendNow(this.getId(), WorkflowSimTags.CLOUDLET_UPDATE);
        if (!processCloudletSubmitHasShown) {
            processCloudletSubmitHasShown = true;
        }
    }

    /**
     * Process a request for the characteristics of a PowerDatacenter.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    @Override
    protected void processResourceCharacteristicsRequest(SimEvent ev) {
        setDatacenterCharacteristicsList(new HashMap<>());
        Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloud Resource List received with "
                + getDatacenterIdsList().size() + " resource(s)");
        for (Integer datacenterId : getDatacenterIdsList()) {
            sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
        }
    }
    
    private JobScheduledResult jobScheduledResultList = new JobScheduledResult();        
    
    public JobScheduledResult getJobScheduledResultList() {
		return jobScheduledResultList;
	}

	public void setJobScheduledResultList(JobScheduledResult jobScheduledResultList) {
		this.jobScheduledResultList = jobScheduledResultList;
	}
       
    private List<TaskScheduledResult> taskScheduledResultList = new ArrayList<TaskScheduledResult>();        
    
    public List<TaskScheduledResult> getTaskScheduledResultList() {
		return taskScheduledResultList;
	}

	public void setTaskScheduledResultList(List<TaskScheduledResult> taskScheduledResultList) {
		this.taskScheduledResultList = taskScheduledResultList;
	}
}
