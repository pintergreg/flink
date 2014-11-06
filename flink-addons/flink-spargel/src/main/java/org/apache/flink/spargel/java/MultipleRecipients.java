package org.apache.flink.spargel.java;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MultipleRecipients<VertexKey extends Comparable<VertexKey>>
		implements Iterable<VertexKey> {

	private List<VertexKey> recipients;

	public MultipleRecipients() {
		recipients = new ArrayList<VertexKey>();
	}

	public void addRecipient(VertexKey recipient) {
		recipients.add(recipient);
	}
	
	public int getNumberOfRecipients(){
		return recipients.size();
	}
	
	
	// Implementing the iterable and the iterator interface

	@Override
	public Iterator<VertexKey> iterator() {
		return recipients.iterator();
	}

}
