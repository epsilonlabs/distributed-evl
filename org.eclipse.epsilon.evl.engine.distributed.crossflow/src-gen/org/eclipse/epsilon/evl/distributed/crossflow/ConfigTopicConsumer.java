package org.eclipse.epsilon.evl.distributed.crossflow;

import javax.jms.Message;

public interface ConfigTopicConsumer {
	
	void consumeConfigTopicWithNotifications(Config config) throws Exception;
	
}