module org.eclipse.epsilon.evl.engine.distributed {
	exports org.eclipse.epsilon.evl.distributed.execute.context;
	exports org.eclipse.epsilon.evl.distributed.execute.data;
	exports org.eclipse.epsilon.evl.distributed;
	exports org.eclipse.epsilon.evl.distributed.launch;
	requires transitive org.eclipse.epsilon.eol.cli;
	requires transitive org.eclipse.epsilon.evl.engine;
}