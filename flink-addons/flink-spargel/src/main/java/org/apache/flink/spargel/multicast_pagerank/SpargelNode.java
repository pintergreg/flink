package org.apache.flink.spargel.multicast_pagerank;

import java.io.Serializable;

public class SpargelNode implements Serializable{
	private static final long serialVersionUID = 1L;

	private long id;
	private double rank;
	private double previousRank = 0.0;
	private long outDegree;
	boolean isSource;


	//constructors
	public SpargelNode(){
		
	}

	public SpargelNode(long id, double rank, long outDegree,
			boolean isSource) {
		super();
		this.id = id;
		this.rank = rank;
		this.outDegree = outDegree;
		this.isSource = isSource;
	}


	//getters, setters
	public long getId() {
		return id;
	}
	public double getPreviousRank() {
		return previousRank;
	}

	public void setPreviousRank(double previousRank) {
		this.previousRank = previousRank;
	}

	
	public double getRank() {
		return rank;
	}

	public void setRank(double rank) {
		this.rank = rank;
	}


	public long getOutDegree() {
		return outDegree;
	}
	
	@Override
	public String toString() {
		return "SpargelNode [id=" + id + ", rank=" + rank + ", outDegree="
				+ outDegree + ", isSource=" + isSource + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + (isSource ? 1231 : 1237);
		result = prime * result + (int) (outDegree ^ (outDegree >>> 32));
		long temp;
		temp = Double.doubleToLongBits(rank);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		SpargelNode other = (SpargelNode) obj;
		if (id != other.id) {
			return false;
		}
		if (isSource != other.isSource) {
			return false;
		}
		if (outDegree != other.outDegree) {
			return false;
		}
		if (Double.doubleToLongBits(rank) != Double
				.doubleToLongBits(other.rank)) {
			return false;
		}
		if (Double.doubleToLongBits(previousRank) != Double
				.doubleToLongBits(other.previousRank)) {
			return false;
		}
		return true;
	}

}
