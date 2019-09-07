module org.eclipse.epsilon.evl.engine.distributed.flink {
	exports org.eclipse.epsilon.evl.distributed.flink.atomic;
	exports org.eclipse.epsilon.evl.distributed.flink;
	exports org.eclipse.epsilon.evl.distributed.flink.execute.context;
	exports org.eclipse.epsilon.evl.distributed.flink.format;
	exports org.eclipse.epsilon.evl.distributed.flink.launch;
	exports org.eclipse.epsilon.evl.distributed.flink.batch;
	requires transitive org.eclipse.epsilon.evl.engine.distributed;
}