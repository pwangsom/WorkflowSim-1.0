package org.workflowsim.scheduling.mapreduce;

public class MMHost2MapReduceAlgorithm extends DelayMapReduceAlgorithm {
	
	public MMHost2MapReduceAlgorithm() {
		super();
		this.satisfyLocality = 1;
		this.delayCount = 2;
	}
	
}
