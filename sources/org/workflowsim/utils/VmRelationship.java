package org.workflowsim.utils;

import org.cloudbus.cloudsim.Vm;

public class VmRelationship {
    
/*    
	private static Map<Integer, List<Integer>> mapOfVMRelationship;
    
    static{
    	mapOfVMRelationship = new HashMap<Integer, List<Integer>>();

    	//Column													= {0, 1, 2, 3, 4, 5, 6, 7};    	
    	List<Integer> listOfVm0 = new ArrayList<Integer>(Arrays.asList(0, 2, 3, 3, 1, 2, 3, 3));
    	List<Integer> listOfVm1 = new ArrayList<Integer>(Arrays.asList(2, 0, 3, 3, 2, 1, 3, 3));
    	List<Integer> listOfVm2 = new ArrayList<Integer>(Arrays.asList(3, 3, 0, 2, 3, 3, 1, 2));
    	List<Integer> listOfVm3 = new ArrayList<Integer>(Arrays.asList(3, 3, 2, 0, 3, 3, 2, 1));
    	List<Integer> listOfVm4 = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 3, 0, 2, 3, 3));
    	List<Integer> listOfVm5 = new ArrayList<Integer>(Arrays.asList(2, 1, 3, 3, 1, 0, 3, 3));
    	List<Integer> listOfVm6 = new ArrayList<Integer>(Arrays.asList(3, 3, 1, 2, 3, 3, 0, 2));
    	List<Integer> listOfVm7 = new ArrayList<Integer>(Arrays.asList(3, 3, 2, 1, 3, 3, 2, 0));
    	
    	mapOfVMRelationship.put(0, listOfVm0);
    	mapOfVMRelationship.put(1, listOfVm1);
    	mapOfVMRelationship.put(2, listOfVm2);
    	mapOfVMRelationship.put(3, listOfVm3);
    	mapOfVMRelationship.put(4, listOfVm4);
    	mapOfVMRelationship.put(5, listOfVm5);
    	mapOfVMRelationship.put(6, listOfVm6);
    	mapOfVMRelationship.put(7, listOfVm7);    	
}*/
    
    public static LocalityType getRelationshipType(Vm basedVm, Vm findRelationshipVm){    	
    	return getRelationshipType(basedVm.getId(), findRelationshipVm.getId());
    }
    
    public static LocalityType getRelationshipType(int basedVmId, int findRelationshipVmId){    	
    	if(isTheSameNode(basedVmId, findRelationshipVmId)){
    		return LocalityType.VM_LOCALITY;
    	} else if(isTheSameHost(basedVmId, findRelationshipVmId)){
    		return LocalityType.HOST_LOCALITY;
    	} else if(isTheSameRack(basedVmId, findRelationshipVmId)){
    		return LocalityType.RACK_LOCALITY;
    	} else {
    		return LocalityType.REMOTE_LOCALITY;
    	}
    }
    
    private static boolean isTheSameNode(int first, int second){
    	if(first == second){
    		return true;
    	} else {
    		return false;
    	}
    }
    
    private static boolean isTheSameHost(int first, int second){
    	first = first%MapReduceParameter.NO_VMS;
    	second = second%MapReduceParameter.NO_VMS;
    	
    	if(first == second){
    		return true;
    	} else {
    		return false;
    	}
    }
    
    private static boolean isTheSameRack(int first, int second){
		first = first%MapReduceParameter.NO_VMS;
		second = second%MapReduceParameter.NO_VMS;
		
		first = first>>1;
		second = second>>1;
		
		if(first == second){
			return true;
		} else {	
			return false;
		}
    }
    
/*    public static LocalityType getRelationshipType(int basedVmId, int findRelationshipVmId){
    	LocalityType type = null;
    	
    	Integer mappingType = mapOfVMRelationship.get(basedVmId).get(findRelationshipVmId);
    	
    	switch(mappingType){
    	case 0: 
    		type = LocalityType.VM_LOCALITY;
    		break;
    	case 1:
    		type = LocalityType.HOST_LOCALITY;
    		break;
    	case 2:
    		type = LocalityType.RACK_LOCALITY;
    		break;
    	default:
    		type = LocalityType.REMOTE_LOCALITY;
    		break;
    	}
    	
    	return type;
    }*/
    
    public static LocalityType getRelationshipType(int basedVmId, int[] findRelationshipVmId){
		int relation = 3;
		
		for (int i = 0; i < findRelationshipVmId.length; i++) {
			if (findRelationshipVmId[i] > -1 && findRelationshipVmId[i] < MapReduceParameter.NO_VMS) {
				int distance = VmRelationship.getRelationshipType(basedVmId, findRelationshipVmId[i]).distance();
				if (distance < relation) {
					relation = distance;
				}
			} else {
				relation = 4;
			}
		}
		
		return LocalityType.fromDistance(relation);
    }
}
