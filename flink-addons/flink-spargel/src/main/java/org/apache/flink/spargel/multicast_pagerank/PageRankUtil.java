package org.apache.flink.spargel.multicast_pagerank;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//import com.carrotsearch.hppc.LongArrayList;

public class PageRankUtil {
	public static final String NUMOFPAGES = "NUMOFPAGES";
	public static final String VALUE_COLLECTED_BY_SINKS = "valueCollectedBySinks";
	public static final String MAX_RANK_CHANGE = "maxRankChange";

	public static DataSet<Tuple1<Long>> getNumOfPages(DataSet<Long> nodes) {
		DataSet<Tuple1<Long>> numOfPages = nodes.map(new MapFunction<Long, Tuple1<Long>>() {
			@Override
			public Tuple1<Long> map(Long value) throws Exception {
				return new Tuple1<Long>(1L);
			}
		}).sum(0).name("Compute the number of pages");
		return numOfPages;
	}

	public static DataSet<Tuple2<Long, long[]>> createNeighbourList(DataSet<Long> nodes,
			DataSet<Tuple2<Long, Long>> edges, final boolean transposeEdges,
			final boolean emptyListAllowed) {
		String name;

		if (transposeEdges) {
			edges = edges.project(1, 0).types(Long.class, Long.class);
			name = "InNeighbour list";
		} else {
			name = "OutNeighbour list";
		}

		// sinksAllowed = false;
		DataSet<Tuple2<Long, long[]>> neighbourList = edges
				.groupBy(0)
				// we collect the nonempty neighbour lists
				.reduceGroup(
						new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, long[]>>() {

							private static final long serialVersionUID = 1L;
							private ArrayList<Long> nodeList = new ArrayList<Long>();
							//private  LongArrayList nodeList = new LongArrayList();

							@Override
							public void reduce(
									Iterable<Tuple2<Long, Long>> values,
									Collector<Tuple2<Long, long[]>> out)
									throws Exception {
								// for some reason I need to call the constructor here
								//LongArrayList nodeList = new LongArrayList();
								// we clear the list
								//nodeList.elementsCount = 0;
								nodeList.clear();
								Iterator<Tuple2<Long, Long>> it = values
										.iterator();
								Tuple2<Long, Long> n = it.next();
								Long id = n.f0;
								nodeList.add(n.f1);
								while (it.hasNext()) {
									n = it.next();
									nodeList.add(n.f1);
								}
								// Let us copy out
								int size = nodeList.size();
								long[] nodeArray = new long[size];
								//System.arraycopy(nodeList.buffer, 0, nodeArray, 0, size);
//								for (int i = 0; i < size; ++i) {
//									nodeArray[i] = nodeListBuffer[i];
//								}
								int i = 0;
					//
								for (long v: nodeList) {
									nodeArray[i] = v;
									++i;
								}
								//nodeList.toArray(nodeArray);
								out.collect(new Tuple2<Long, long[]>(id,
										nodeArray));
							}
						}).name("nonempty neighbour list")
				// next we add the empty neighbour lists
				.coGroup(nodes)
				.where(0)
				.equalTo(new KeySelector<Long, Long>() {
					@Override
					public Long getKey(Long value) throws Exception {
						return value;
					}
				})
				.with(new CoGroupFunction<Tuple2<Long, long[]>, Long, Tuple2<Long, long[]>>() {
					private Iterator<Tuple2<Long, long[]>> it1; 
					@Override
					public void coGroup(Iterable<Tuple2<Long, long[]>> first,
							Iterable<Long> second,
							Collector<Tuple2<Long, long[]>> out)
							throws Exception {
						it1 = first.iterator();
						Iterator<Long> it2 = second.iterator();
						if (it1.hasNext()) {
							out.collect(it1.next());
						} else {
							Long nodeId = it2.next();
							if (!emptyListAllowed) {
								String message;
								if (transposeEdges) {
									message = "Sources ";
								} else {
									message = "Sinks ";
								}
								message += "(" + nodeId + ")";
								message += " are not allowed in the current implementation.";
								throw new RuntimeException(message);
							}
							out.collect(new Tuple2<Long, long[]>(nodeId,
									new long[0]));
						}
					}
				}).name(name);
		return neighbourList;
	}
}