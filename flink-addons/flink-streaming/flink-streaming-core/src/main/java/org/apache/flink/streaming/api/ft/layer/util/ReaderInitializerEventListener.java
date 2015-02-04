package org.apache.flink.streaming.api.ft.layer.util;

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.util.event.EventListener;

public class ReaderInitializerEventListener implements EventListener<TaskEvent> {

	private boolean ftConnectionIsInitialized;
	private Thread streamVertexThread;

	public ReaderInitializerEventListener() {
		this.ftConnectionIsInitialized = false;
	}

	public void setStreamVertexThread(Thread thread) {
		streamVertexThread = thread;
	}

	@Override
	public void onEvent(TaskEvent event) {
		ftConnectionIsInitialized = true;

		if (streamVertexThread != null) {
			streamVertexThread.notify();
		}
	}

	public boolean isInitialized() {
		return ftConnectionIsInitialized;
	}
}
