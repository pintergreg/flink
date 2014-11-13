package org.apache.flink.runtime.io.network.multicast;

import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;

public interface MulticastChannelSelector extends ChannelSelector<SerializationDelegate<MulticastMessage>> {
}
