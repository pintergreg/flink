package org.apache.flink.streaming.api.ft.context;

import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.collector.ft.PersistenceCollector;
import org.apache.flink.streaming.api.collector.ft.SourceAckerCollector;
import org.apache.flink.streaming.api.ft.layer.util.SemiDeserializedStreamRecord;
import org.apache.flink.streaming.io.StreamRecordWriter;
import org.apache.flink.streaming.partitioner.PersistencePartitioner;

public class FTSourceContext extends FTContext {

	private PersistenceCollector persistenceCollector;
	private StreamRecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>> ftWriter;

	protected FTSourceContext() {
	}

	public FTSourceContext(AbstractInvokable vertex) {
		PersistencePartitioner partitioner = new PersistencePartitioner();
		ftWriter = new StreamRecordWriter<SerializationDelegate<SemiDeserializedStreamRecord>>(vertex, partitioner, 100L);

		persistenceCollector = new PersistenceCollector(ftWriter);
		ackerCollector = new SourceAckerCollector(ftWriter);
	}

	@Override
	public void initialize() {
		ftWriter.initializeSerializers();
	}

	public void persist(SemiDeserializedStreamRecord semiDeserializedStreamRecord) {
		persistenceCollector.collect(semiDeserializedStreamRecord);
	}
}
