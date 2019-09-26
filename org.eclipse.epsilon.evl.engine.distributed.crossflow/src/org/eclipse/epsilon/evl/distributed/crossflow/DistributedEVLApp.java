package org.eclipse.epsilon.evl.distributed.crossflow;

import org.eclipse.scava.crossflow.runtime.Mode;

public class DistributedEVLApp {

	public static void main(String[] args) throws Exception {
		
		DistributedEVL master = new DistributedEVL(Mode.MASTER);
		master.createBroker(true);
		master.setMaster("localhost");
		
		//master.setParallelization(4);
		
		//master.setInputDirectory(new File("experiment/in"));
		//master.setOutputDirectory(new File("experiment/out"));
		
		master.setInstanceId("Example DistributedEVL Instance");
		master.setName("DistributedEVL");
		
		master.run();
		
		master.awaitTermination();
		
		System.out.println("Done");
		
	}
	
}
