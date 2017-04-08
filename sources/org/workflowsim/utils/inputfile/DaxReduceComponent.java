package org.workflowsim.utils.inputfile;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.util.JavaUtil;

public class DaxReduceComponent {
	
	private List<DaxReduceComponent> daxReduceList;
	private List<DaxMapComponent> daxMapList;
	
	private String jobId = "ID";
	private String nameSpace = "MapReduce";
	private String name = "map_task";
	private String version = "1.0";
	private String runtime = "100.00";

	private String outputFile;
	private String linkOutput = "output";
	private String register = "true";
	private String transfer = "true";
	private String optional = "false";
	private String type = "data";
	private String outputSize = "5242880";
	
	public DaxReduceComponent(){
		this.daxReduceList = new ArrayList<DaxReduceComponent>();
	}
	
	public DaxReduceComponent(int jobId){
		this.jobId = this.jobId + String.format("%06d", jobId);
		this.outputFile = "output_" + this.jobId;
	}
	
	public DaxReduceComponent(int jobId, String runtime){
		this(jobId);
		this.runtime = runtime;
	}
	
	public void add(DaxReduceComponent reduce){
		if(JavaUtil.isNull(daxReduceList)) daxReduceList = new ArrayList<DaxReduceComponent>();
		this.daxReduceList.add(reduce);
	}
	
	public void add(DaxMapComponent map){
		if(JavaUtil.isNull(daxMapList)) daxMapList = new ArrayList<DaxMapComponent>();
		this.daxMapList.add(map);
	}
	
	public void setDaxMapComponentList(List<DaxMapComponent> mapList){
		this.daxMapList = mapList;
	}
	
	public List<String> createChildLinesOfAllDaxReduce(){
		List<String> childLines = new ArrayList<String>();
		
		if(!JavaUtil.isNull(daxReduceList) && !JavaUtil.isNull(daxMapList)){			
			for(DaxReduceComponent reducer : daxReduceList){
				childLines.add(String.format("<child ref=\"%s\">", reducer.jobId));
				
				for(DaxMapComponent map : daxMapList){
					childLines.add(String.format("<parent ref=\"%s\"/>", map.getJobId()));
				}
				childLines.add("</child>");
			}
		}
		
		return childLines;
	} 
	
	public List<String> createJobLinesOfAllDaxReduce(){
		List<String> reduceLines = new ArrayList<String>();
		
		if(!JavaUtil.isNull(daxReduceList) && !JavaUtil.isNull(daxMapList)){			
			for(DaxReduceComponent reducer : daxReduceList){
				reduceLines.addAll(createJobLinesOfEachDaxReduce(reducer, daxMapList));
			}
		}
		
		return reduceLines;
	}
	
	public List<String> createJobLinesOfEachDaxReduce(DaxReduceComponent reduce, List<DaxMapComponent> mapList){
		List<String> reduceLines = new ArrayList<String>();
		
		reduceLines.add(getJobFirstLine(reduce));
		
		for(DaxMapComponent map : daxMapList){
			reduceLines.add(map.toStringForDaxReduce());
		}
		
		reduceLines.add(getJobOutPutLine(reduce));
		reduceLines.add("</job>");
		
		return reduceLines;
	}
	
	private String getJobFirstLine(DaxReduceComponent reduce){
		return String.format("<job id=\"%s\" namespace=\"%s\" name=\"%s\" version=\"%s\" runtime=\"%s\">", 
				reduce.jobId, reduce.nameSpace, reduce.name, reduce.version, reduce.runtime);
	}
	
	private String getJobOutPutLine(DaxReduceComponent reduce){
		return String.format("<uses file=\"%s\" link=\"%s\" register=\"%s\" transfer=\"%s\" optional=\"%s\" type=\"%s\" size=\"%s\"/>", 
				reduce.outputFile, reduce.linkOutput, reduce.register, reduce.transfer, reduce.optional, reduce.type, reduce.outputSize);
	}
	
	private String getChildFirstLine(DaxReduceComponent reduce){
		return String.format("<child ref=\"%s\">", reduce.jobId);
	}

}
