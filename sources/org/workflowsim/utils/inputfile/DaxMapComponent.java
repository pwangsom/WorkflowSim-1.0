package org.workflowsim.utils.inputfile;

/*
	<job id="ID00001" namespace="MapReduce" name="map_task" version="1.0" runtime="140.5">
	<uses file="hdfs_file_map_001" link="input" register="true" transfer="true" optional="false" type="data" size="67108864"/>
	<uses file="hdfs_file_map_result_001" link="output" register="true" transfer="true" optional="false" type="data" size="5242880"/>
	</job>
*/

public class DaxMapComponent {
	
	private String jobId = "ID";
	private String nameSpace = "MapReduce";
	private String name = "map_task";
	private String version = "1.0";
	private String runtime = "200.00";
	
	private String inputFile;
	private String linkInput = "input";
	private String register = "true";
	private String transfer = "true";
	private String optional = "false";
	private String type = "data";
	private String inputSize = "67108864";	

	private String outputFile;
	private String linkOutput = "output";
	private String outputSize = "5242880";
	
	public DaxMapComponent(int jobId){
		this.jobId = this.jobId + String.format("%06d", jobId);
		this.inputFile = "input_" + this.jobId;
		this.outputFile = "output_" + this.jobId;
	}
	
	public DaxMapComponent(int jobId, String runtime){
		this(jobId);
		this.runtime = runtime;
	}
	
	public String toString(){
		return String.format("<job id=\"%s\" namespace=\"%s\" name=\"%s\" version=\"%s\" runtime=\"%s\">"
							+ "<uses file=\"%s\" link=\"%s\" register=\"%s\" transfer=\"%s\" optional=\"%s\" type=\"%s\" size=\"%s\"/>"
							+ "<uses file=\"%s\" link=\"%s\" register=\"%s\" transfer=\"%s\" optional=\"%s\" type=\"%s\" size=\"%s\"/>"
							+ "</job>", 
							this.jobId, this.nameSpace, this.name, this.version, this.runtime,
							this.inputFile, this.linkInput, this.register, this.transfer, this.optional, this.type, this.inputSize,
							this.outputFile, this.linkOutput, this.register, this.transfer, this.optional, this.type, this.outputSize);
	}
	
	public String toStringForDaxReduce(){
		return String.format("<uses file=\"%s\" link=\"%s\" register=\"%s\" transfer=\"%s\" optional=\"%s\" type=\"%s\" size=\"%s\"/>", 
							this.outputFile, this.linkInput, this.register, this.transfer, this.optional, this.type, this.outputSize);
	}
	
	public String getJobId(){
		return this.jobId;
	}
}
