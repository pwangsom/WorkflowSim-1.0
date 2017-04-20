package org.workflowsim.examples.mapreduce;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Log;
import org.workflowsim.utils.JobScheduledResult;
import org.workflowsim.utils.Parameters;

public class MapReduceExperiment {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		List<Parameters.SchedulingAlgorithm> algorithmsList = new ArrayList<Parameters.SchedulingAlgorithm>();
		algorithmsList.add(Parameters.SchedulingAlgorithm.FCFS_MR);
		algorithmsList.add(Parameters.SchedulingAlgorithm.DATA_MR);
		algorithmsList.add(Parameters.SchedulingAlgorithm.DELAY_MR);
		algorithmsList.add(Parameters.SchedulingAlgorithm.MAXMIN);
		algorithmsList.add(Parameters.SchedulingAlgorithm.MINMIN);
		
		int noOfReduceTask = 16;
		int[] noOfMapTaskList = {2500, 5000, 7500, 10000};
		
		for (int f = 0; f < noOfMapTaskList.length; f++) {
			
			for (Parameters.SchedulingAlgorithm algor : algorithmsList) {
				
				List<JobScheduledResult> resultList = new ArrayList<JobScheduledResult>();

				for (int i = 1; i <= 3; i++) {
					String fileName = String.format("mr_%dm_%dr_v%d.xml", noOfMapTaskList[f], noOfReduceTask, i);

					Log.printLine("Starting experiment: Algorthm Name: " + algor + "; File name: " + fileName
							+ "; Reps no: " + i);
					
					resultList.add(runExperiments(algor, fileName));
				}
				
				writeOutputFile(algor, noOfMapTaskList[f], resultList);
				
			}
						
		}
	}
	
	private static JobScheduledResult runExperiments(Parameters.SchedulingAlgorithm testAlgor, String fileName){
		MapReduceSchedulingAlgorithmsInstance instance = new MapReduceSchedulingAlgorithmsInstance(testAlgor, fileName);
		instance.runAlgorithms();
		return instance.getResult();
	}
	
	private static void writeOutputFile(Parameters.SchedulingAlgorithm testAlgor, int noOfMapTask, List<JobScheduledResult> results){
		
		String pathFile = String.format("./mapreduce/output/%d_%s.csv", noOfMapTask, testAlgor);		

		List<String> lines = new ArrayList<String>();
		lines.addAll(getResultLines(results));
		
		lines.add("");
		try {
			Files.write(Paths.get(pathFile), lines, UTF_8, CREATE, CREATE);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	private static List<String> getResultLines(List<JobScheduledResult> results){
		List<String> lines = new ArrayList<String>();
		DecimalFormat dft = new DecimalFormat("###.##");
		
		String noOfTasks = ("NoOfTasks,");
		
		String noOfVm = ("NoOfVM,");
		String noOfHosts = ("NoOfHost,");
		String noOfRack = ("NoOfRack,");
		String noOfRemote = ("NoOfRemote,");		

		String ratioOfVm = ("RatioOfVM,");
		String ratioOfHosts = ("RatioOfHost,");
		String ratioOfRack = ("RatioOfRack,");
		String ratioOfRemote = ("RatioOfRemote,");
		
		String executionTime = ("ExecutionTime,");
		
		for(JobScheduledResult result : results){
			noOfTasks = noOfTasks + dft.format(result.getNumOfTask()) + ",";
			
			noOfVm = noOfVm + dft.format(result.getNumOfVmLocal()) + ",";
			noOfHosts = noOfHosts + dft.format(result.getNumOfHostLocal()) + ",";
			noOfRack = noOfRack + dft.format(result.getNumOfRackLocal()) + ",";
			noOfRemote = noOfRemote + dft.format(result.getNumOfRemoteLocal()) + ",";			

			ratioOfVm = ratioOfVm + dft.format(result.getVmLocalRatio()) + ",";
			ratioOfHosts = ratioOfHosts + dft.format(result.getHostLocalRatio()) + ",";
			ratioOfRack = ratioOfRack + dft.format(result.getRackLocalRatio()) + ",";
			ratioOfRemote = ratioOfRemote + dft.format(result.getRemoteLocalRatio()) + ",";			

			executionTime = executionTime + dft.format(result.getTotalExecutetionTime()) + ",";
		}
		
		lines.add(noOfTasks);
		
		lines.add(noOfVm);
		lines.add(noOfHosts);
		lines.add(noOfRack);
		lines.add(noOfRemote);		

		lines.add(ratioOfVm);
		lines.add(ratioOfHosts);
		lines.add(ratioOfRack);
		lines.add(ratioOfRemote);		

		lines.add(executionTime);
		
		return lines;
	}

}
