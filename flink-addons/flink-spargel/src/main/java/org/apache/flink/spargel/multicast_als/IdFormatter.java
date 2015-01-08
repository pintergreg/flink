package org.apache.flink.spargel.multicast_als;

public class IdFormatter {

	public static int getVertexId(boolean partOfQ, int originalId) {
		return (partOfQ ? 2 * originalId : 2 * originalId + 1);
	}

	public static int getOriginalId(int vertexId) {
		return (vertexId % 2 == 0 ? vertexId / 2 : (vertexId - 1) / 2);
	}

	public static int getMultiVertexIdFromVertexId(int vertexId,
			int numOfSubTasks) {
		boolean partOfQ = (vertexId % 2 == 0);
		if (partOfQ) {
			// return ((vertexId / 2) % numOfSubTasks) * 2;
			return (getOriginalId(vertexId) % numOfSubTasks) * 2;
		} else {
			// return (((vertexId - 1) / 2) % numOfSubTasks) * 2 + 1;
			return (getOriginalId(vertexId) % numOfSubTasks) * 2 + 1;
		}
	}
}
