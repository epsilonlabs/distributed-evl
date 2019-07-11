module org.eclipse.epsilon.evl.engine.distributed.crossflow {
	exports org.eclipse.epsilon.evl.distributed.crossflow.launch;
	exports org.eclipse.epsilon.evl.distributed.crossflow.batch;
	exports org.eclipse.epsilon.evl.distributed.crossflow.atomic;
	exports org.eclipse.epsilon.evl.distributed.crossflow;
	requires activemq.all;
	requires java.naming;
	requires jcommander;
	requires transitive org.eclipse.scava.crossflow.runtime;
	requires transitive org.eclipse.epsilon.evl.engine.distributed;
}