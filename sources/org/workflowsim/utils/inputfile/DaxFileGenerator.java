package org.workflowsim.utils.inputfile;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.cloudbus.cloudsim.Log;

public class DaxFileGenerator {
	// TODO Auto-generated method stub
	
	private final static int numOfMapTask = 10000;
	private final static int numOfReduceTask = 16;
	
	private final static double minMapRuntime = 150.00;
	private final static double maxMapRuntime = 199.99;		

	private final static double minReduceRuntime = 50.00;
	private final static double maxReduceRuntime = 79.99;
	
	private final static String xmlVersion = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
	private final static String xsdInfo = "<adag xmlns=\"http://pegasus.isi.edu/schema/DAX\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
						 + " xsi:schemaLocation=\"http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-2.1.xsd\" version=\"2.1\""
						 + " count=\"1\" index=\"0\" name=\"test\" jobCount=\"%s\" fileCount=\"0\" childCount=\"%s\">";
	
	private final static String endTagXml = "</adag>";

	public static void main(String[] args) {
		
		try {
			
			String map = String.valueOf(numOfMapTask);
			String red = String.valueOf(numOfReduceTask);
			int version = 3;
			
			for(int i = 1; i <= version; i++){
				String pathFile = String.format("./mapreduce/input/mr_%sm_%sr_v%s.xml", map, red, String.valueOf(i));	
				genDaxFile(pathFile);	
				Log.printLine(pathFile);
			}
									
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	private static void genDaxFile(String pathFile){
		try {	
			
			DecimalFormat dft = new DecimalFormat("###.##");	
			double random;
			
			List<String> lines = new ArrayList<String>();
			
			lines.add(xmlVersion);			
			String xsd = String.format(xsdInfo, String.valueOf(numOfMapTask + numOfReduceTask), String.valueOf(numOfReduceTask));
			lines.add(xsd);
			
			DaxReduceComponent reducerManager = new DaxReduceComponent();
			
			for(int i = 1; i <= numOfMapTask; i++){
				
				random = ThreadLocalRandom.current().nextDouble(minMapRuntime, maxMapRuntime);			
				DaxMapComponent mapTask = new DaxMapComponent(i, dft.format(random));				

				reducerManager.add(mapTask);
				
				lines.add(mapTask.toString());
			}
			
			for(int i = numOfMapTask+1; i <= numOfMapTask+numOfReduceTask; i++){				
				random = ThreadLocalRandom.current().nextDouble(minReduceRuntime, maxReduceRuntime);			
				DaxReduceComponent reduceTask = new DaxReduceComponent(i, dft.format(random));	
				
				reducerManager.add(reduceTask);
			}
			
			lines.add("");
			lines.addAll(reducerManager.createJobLinesOfAllDaxReduce());
			lines.add("");
			lines.addAll(reducerManager.createChildLinesOfAllDaxReduce());			
			
			lines.add(endTagXml);
			
			
/*			for(String line : lines){
				Log.printLine(line);
			}	*/	
					
			Files.write(Paths.get(pathFile), lines, UTF_8, CREATE, CREATE);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
