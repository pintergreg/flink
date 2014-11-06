package org.apache.flink.spargel.java;

import static org.junit.Assert.*;

import org.junit.Test;

public class MultipleRecipientsTest {

	@Test
	public void testIterator() {
		MultipleRecipients<String> recipients = 
				new MultipleRecipients<String>();
		String[] expected = {"Alma", "Korte"};
		recipients.addRecipient(expected[0]);
		recipients.addRecipient(expected[1]);
		assertEquals(2, recipients.getNumberOfRecipients());
		{
		//Checking the iterator
		int i = 0;
		for (String m: recipients) {
			assertEquals(expected[i], m);
			++i;
		}
		}
		{
		//ReChecking the iterator
		int i = 0;
		for (String m: recipients) {
			assertEquals(expected[i], m);
			++i;
		}
		}
		
	}

}
