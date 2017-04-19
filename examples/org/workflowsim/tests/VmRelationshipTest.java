package org.workflowsim.tests;

import org.cloudbus.cloudsim.Log;
import org.workflowsim.utils.VmRelationship;

public class VmRelationshipTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		int based = 9;
		int[] dfsFiles = {11, 1, 6};
		
		Log.printLine(VmRelationship.getRelationshipType(based, dfsFiles).toString());

	}

}
