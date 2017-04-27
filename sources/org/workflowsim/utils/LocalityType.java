package org.workflowsim.utils;

public enum LocalityType {
	
/*    VM_LOCALITY(0, 0),
    HOST_LOCALITY(1, 100000),
    RACK_LOCALITY(2, 300000),
    REMOTE_LOCALITY(3, 500000),
	UNDEFINED(4, 0);*/
	
    VM_LOCALITY(0, 0),
    HOST_LOCALITY(1, 67000),
    RACK_LOCALITY(2, 268000),
    REMOTE_LOCALITY(3, 402000),
	UNDEFINED(4, 0);
    
    private int distance;
    private long penaltyLength;
	
	LocalityType(int distance, long penaltyLength){
		this.distance = distance;
		this.penaltyLength = penaltyLength;
	}
	
	public int distance(){
		return this.distance;
	}
	
	public long penaltyLenght(){
		return this.penaltyLength;
	}
	
	public static LocalityType fromDistance(int distance){
		return LocalityType.values()[distance];
	}
	
}
