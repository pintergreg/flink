package org.apache.flink.examples.java.multicast;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.io.network.multicast.MulticastCollector;
import org.apache.flink.runtime.io.network.multicast.MulticastMessage;
import org.apache.flink.util.Collector;

public class MultiCastTest {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

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
		messages.print();

		messages.map(new MapFunction<MulticastMessage, Tuple2<Long, Double>>() {
			@Override
			public Tuple2<Long, Double> map(MulticastMessage value)
					throws Exception {
				return new Tuple2<Long, Double>(value.f0[0], value.f1);
			}
		}).print();

		env.setDegreeOfParallelism(4);
		env.execute("Multicast Test");
	}

}
