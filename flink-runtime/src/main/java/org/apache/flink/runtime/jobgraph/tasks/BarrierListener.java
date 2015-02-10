package org.apache.flink.runtime.jobgraph.tasks;


public interface BarrierListener {

	public void broadcastBarrier(long barrierID);
	
}
