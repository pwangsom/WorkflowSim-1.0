package org.workflowsim.examples.mapreduce;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.cloudbus.cloudsim.Log;
import org.workflowsim.utils.JobScheduledResult;
import org.workflowsim.utils.Parameters;

public class MapReduceExperiment {
	
	private static int replicas = 1;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		// String daxPath = "C://Users/Dell/git/WorkflowSim-1.0/mapreduce/input/";
		String daxPath = "D://Users-Profiles/Peerasak/git/WorkflowSim-1.0/mapreduce/input/";
		
		List<Parameters.SchedulingAlgorithm> algorithmsList = new ArrayList<Parameters.SchedulingAlgorithm>();
		// algorithmsList.add(Parameters.SchedulingAlgorithm.FCFS_MR);
		// algorithmsList.add(Parameters.SchedulingAlgorithm.DATA_MR);
		// algorithmsList.add(Parameters.SchedulingAlgorithm.DELAY_MR);
		// algorithmsList.add(Parameters.SchedulingAlgorithm.MAXMIN);
		// algorithmsList.add(Parameters.SchedulingAlgorithm.MINMIN);
		

		// algorithmsList.add(Parameters.SchedulingAlgorithm.MM_VM_2);
		// algorithmsList.add(Parameters.SchedulingAlgorithm.MM_HO_2);
		// algorithmsList.add(Parameters.SchedulingAlgorithm.DL_VM_4);
		// algorithmsList.add(Parameters.SchedulingAlgorithm.DL_HO_4);		

		algorithmsList.add(Parameters.SchedulingAlgorithm.MM_HO_NEW);
		
		int noOfReduceTask = 16;
		int[] noOfMapTaskList = {1000, 2500};
		// int[] noOfMapTaskList = {7500};
		
		for (int f = 0; f < noOfMapTaskList.length; f++) {
			
			for (Parameters.SchedulingAlgorithm algor : algorithmsList) {
				
				List<JobScheduledResult> resultList = new ArrayList<JobScheduledResult>();

				for (int i = 1; i <= replicas; i++) {
					
					long startTime = System.currentTimeMillis();
					
					Log.printLine("=============================================================================");
					Log.printLine("=========================== Start Experiment ================================");
					
					String fileName = String.format("mr_%dm_%dr_v%d.xml", noOfMapTaskList[f], noOfReduceTask, i);
					Log.printLine(getCurrentDateTimeString() + ": Starting experiment: Algorthm Name: " 
							+ algor + "; File name: " + fileName
							+ "; Reps no: " + i);
					
					resultList.add(runExperiments(algor, daxPath, fileName));
					
				
					Log.printLine(getCurrentDateTimeString() + ": Exiting experiment: Algorthm Name: " 
							+ algor + "; File name: " + fileName
							+ "; Reps no: " + i);
					
					
					long finishTime = System.currentTimeMillis();
					
					Log.printLine("Experiment time: " + getDurationTimeString(startTime, finishTime));

					Log.printLine("=========================== Finish Experiment ===============================");
					Log.printLine("=============================================================================");
					Log.printLine("");
				}
				
				writeOutputFile(algor, noOfMapTaskList[f], resultList);				
			}
						
		}
	}
	
	protected static JobScheduledResult runExperiments(Parameters.SchedulingAlgorithm testAlgor, String daxPath, String fileName){
		MapReduceSchedulingAlgorithmsInstance instance = new MapReduceSchedulingAlgorithmsInstance(testAlgor, daxPath, fileName);
		instance.run();
		return instance.getResult();
	}
	
	protected static void writeOutputFile(Parameters.SchedulingAlgorithm testAlgor, int noOfMapTask, List<JobScheduledResult> results){
		
		String pathFile = String.format("./mapreduce/output/MM_HOST_NEW/Exp4/%d_%s.csv", noOfMapTask, testAlgor);		

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
	
	protected static List<String> getResultLines(List<JobScheduledResult> results){
		List<String> lines = new ArrayList<String>();
		DecimalFormat dft = new DecimalFormat("###.##");
		DecimalFormat dft4 = new DecimalFormat("###.####");
		
		double doubleOfTasks = 0.0;
		double doubleOfVm = 0.0;
		double doubleOfHosts = 0.0;
		double doubleOfRack = 0.0;
		double doubleOfRemote = 0.0;
		
		double doubleOfExecutionTime = 0.0;
		
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
			doubleOfTasks += result.getNumOfTask();
			noOfTasks = noOfTasks + dft.format(result.getNumOfTask()) + ",";
			
			doubleOfVm += result.getNumOfVmLocal();
			doubleOfHosts += result.getNumOfHostLocal();
			doubleOfRack += result.getNumOfRackLocal();
			doubleOfRemote += result.getNumOfRemoteLocal();
			
			noOfVm = noOfVm + dft.format(result.getNumOfVmLocal()) + ",";
			noOfHosts = noOfHosts + dft.format(result.getNumOfHostLocal()) + ",";
			noOfRack = noOfRack + dft.format(result.getNumOfRackLocal()) + ",";
			noOfRemote = noOfRemote + dft.format(result.getNumOfRemoteLocal()) + ",";			

			ratioOfVm = ratioOfVm + dft.format(result.getVmLocalRatio()) + ",";
			ratioOfHosts = ratioOfHosts + dft.format(result.getHostLocalRatio()) + ",";
			ratioOfRack = ratioOfRack + dft.format(result.getRackLocalRatio()) + ",";
			ratioOfRemote = ratioOfRemote + dft.format(result.getRemoteLocalRatio()) + ",";			

			doubleOfExecutionTime += result.getTotalExecutetionTime();
			executionTime = executionTime + dft.format(result.getTotalExecutetionTime()) + ",";
		}
		
/*		// Column Total
		noOfTasks = noOfTasks + dft.format(doubleOfTasks) + ",";
		
		noOfVm = noOfVm + dft.format(doubleOfVm) + ",";
		noOfHosts = noOfHosts + dft.format(doubleOfHosts) + ",";
		noOfRack = noOfRack + dft.format(doubleOfRack) + ",";
		noOfRemote = noOfRemote + dft.format(doubleOfRemote) + ",";
		
		ratioOfVm = ratioOfVm + dft.format((doubleOfVm/doubleOfTasks)*100) + ",";
		ratioOfHosts = ratioOfHosts + dft.format((doubleOfHosts/doubleOfTasks)*100) + ",";
		ratioOfRack = ratioOfRack + dft.format((doubleOfRack/doubleOfTasks)*100) + ",";
		ratioOfRemote = ratioOfRemote + dft.format((doubleOfRemote/doubleOfTasks)*100) + ",";		
		
		executionTime = executionTime + dft.format(doubleOfExecutionTime) + ",";
		
		//Column AVG
		noOfTasks = noOfTasks + dft.format(doubleOfTasks/replicas);
		
		noOfVm = noOfVm + dft.format(doubleOfVm/replicas);
		noOfHosts = noOfHosts + dft.format(doubleOfHosts/replicas);
		noOfRack = noOfRack + dft.format(doubleOfRack/replicas);
		noOfRemote = noOfRemote + dft.format(doubleOfRemote/replicas);
		
		ratioOfVm = ratioOfVm + dft4.format((doubleOfVm/replicas)/(doubleOfTasks/replicas));
		ratioOfHosts = ratioOfHosts + dft4.format((doubleOfHosts/replicas)/(doubleOfTasks/replicas));
		ratioOfRack = ratioOfRack + dft4.format((doubleOfRack/replicas)/(doubleOfTasks/replicas));
		ratioOfRemote = ratioOfRemote + dft4.format((doubleOfRemote/replicas)/(doubleOfTasks/replicas));		
		
		executionTime = executionTime + dft.format(doubleOfExecutionTime/replicas);		
*/		
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
	
	private static String getCurrentDateTimeString(){
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		return dateFormat.format(date); //2016/11/16 12:08:43
	}
	
	private static String getDurationTimeString(long startTime, long finishTime){
		
		Date d1 = new Date(startTime);
		Date d2 = new Date(finishTime);

		//in milliseconds
		long diff = d2.getTime() - d1.getTime();

		long diffSeconds = diff / 1000 % 60;
		long diffMinutes = diff / (60 * 1000) % 60;
		long diffHours = diff / (60 * 60 * 1000) % 24;
		long diffDays = diff / (24 * 60 * 60 * 1000);
		
		return diffDays + " days, " + diffHours + " hours, " + diffMinutes + " minutes, " + diffSeconds + " seconds.";
	}

}
