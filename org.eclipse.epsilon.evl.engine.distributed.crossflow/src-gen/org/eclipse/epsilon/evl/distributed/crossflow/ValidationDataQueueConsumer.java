package org.eclipse.epsilon.evl.distributed.crossflow;

import javax.jms.Message;

public interface ValidationDataQueueConsumer {
	
	void consumeValidationDataQueueWithNotifications(ValidationData validationData) throws Exception;
	
}