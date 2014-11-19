package org.apache.flink.examples.java.multicast;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.io.network.multicast.MulticastMessage;
import org.apache.flink.util.Collector;

public class MulticastGroupByTest {

	public static void main(String[] args) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		
//		//What is the default port? 6023?
//		final ExecutionEnvironment env = ExecutionEnvironment
//				.createRemoteEnvironment("127.0.0.1",6023, jarFiles);

		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Double, long[]>> data = env
				.fromElements(new Tuple3<Long, Double, long[]>(0L, 0.3,
						new long[] { 0L, 1L, 2L, 3L }),
						new Tuple3<Long, Double, long[]>(1L, 0.1, new long[] {
								0L, 2L }), new Tuple3<Long, Double, long[]>(2L,
								0.2, new long[] { 0L, 1L }));

		System.out.println("Original data:");
		data.print();

		DataSet<MulticastMessage> messages = data
				.flatMap(new FlatMapFunction<Tuple3<Long, Double, long[]>, MulticastMessage>() {
					@Override
					public void flatMap(Tuple3<Long, Double, long[]> value,
							Collector<MulticastMessage> out) throws Exception {
						out.collect(new MulticastMessage(value.f2, value.f1));
					}
				});
		// if this print is commented then only vertex 0 gets messages! WHY?
		//messages.print();

		//NOTE: it is not an efficient exmaple, here is a collect for the original data, which we tried to avoid with multicast
		DataSet<Tuple2<Long, Double>> originalMessage = messages.groupBy(new KeySelector<MulticastMessage, Long>() {
			
			@Override
			public Long getKey(MulticastMessage value) throws Exception {
				return value.f0[0];
			}}).reduceGroup(new GroupReduceFunction<MulticastMessage, Tuple2<Long,Double>>() {

				private Tuple2<Long,Double> record = new Tuple2<Long, Double>();
				@Override
				public void reduce(Iterable<MulticastMessage> values,
						Collector<Tuple2<Long, Double>> out) throws Exception {
					MulticastMessage value = values.iterator().next();
					record.setFields(value.f0[0], value.f1);
					out.collect(record);
				}
			});
		originalMessage.print();

		env.setDegreeOfParallelism(4);
		env.execute("Multicast Test");
	}

}

