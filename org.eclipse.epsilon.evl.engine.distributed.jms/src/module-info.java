module org.eclipse.epsilon.evl.engine.distributed.jms {exports org.eclipse.epsilon.evl.distributed.jms.launch;
	exports org.eclipse.epsilon.evl.distributed.jms;
	exports org.eclipse.epsilon.evl.distributed.jms.atomic;
	exports org.eclipse.epsilon.evl.distributed.jms.batch;
	requires transitive org.eclipse.epsilon.evl.engine.distributed;
}