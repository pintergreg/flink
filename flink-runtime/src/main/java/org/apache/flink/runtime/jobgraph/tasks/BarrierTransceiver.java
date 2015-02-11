package org.apache.flink.runtime.jobgraph.tasks;


public interface BarrierTransceiver {

	public void broadcastBarrier(long barrierID);
	
	public void confirmBarrier(long barrierID);
	
}
