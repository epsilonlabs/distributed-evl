package org.eclipse.epsilon.evl.distributed.crossflow;

import javax.jms.Message;

public interface ValidationOutputConsumer {
	
	void consumeValidationOutputWithNotifications(ValidationResult validationResult) throws Exception;
	
}