/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.launch;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.execute.control.ExecutionController;
import org.eclipse.epsilon.eol.execute.control.ExecutionProfiler;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class DistributedEvlRunConfigurationSlave extends DistributedEvlRunConfiguration {

	public static class Builder<R extends DistributedEvlRunConfigurationSlave, B extends Builder<R, B>> extends DistributedEvlRunConfiguration.Builder<R, B> {
		@Override
		protected EvlModuleDistributedSlave createModule() {
			return new EvlModuleDistributedSlave();
		}
		
		protected Builder() {
			super();
		}
		protected Builder(Class<R> runConfigClass) {
			super(runConfigClass);
		}
	}
	
	/**
	 * This constructor is to be called by workers as a convenient
	 * data holder for initializing Epsilon.
	 * 
	 * @param evlFile
	 * @param modelsAndProperties
	 * @param evlModule
	 * @param parameters
	 */
	public DistributedEvlRunConfigurationSlave(
		String basePath,
		Path evlFile,
		Map<IModel, StringProperties> modelsAndProperties,
		EvlModuleDistributedSlave evlModule,
		Map<String, Object> parameters) {
			super(Builder()
				.withBasePath(basePath)
				.withScript(evlFile)
				.withModels(modelsAndProperties)
				.withModule(evlModule)
				.withParameters(parameters)
				.withProfiling()
			);
	}
	

	/**
	 * Convenience method for serializing the profiling information of a
	 * slave worker to be sent to the master.
	 * 
	 * @return A serializable representation of {@link ModuleElementProfiler}.
	 */
	public HashMap<String, java.time.Duration> getSerializableRuleExecutionTimes() {
		ExecutionController controller = getModule().getContext().getExecutorFactory().getExecutionController();
		if (controller instanceof ExecutionProfiler) {
			return ((ExecutionProfiler) controller)
				.getExecutionTimes()
				.entrySet().stream()
				.collect(Collectors.toMap(
					e -> e.getKey().toString(),
					Map.Entry::getValue,
					(t1, t2) -> t1.plus(t2),
					HashMap::new
				));
		}
		return null;
	}
	
	@Override
	public EvlModuleDistributedSlave getModule() {
		return (EvlModuleDistributedSlave) super.getModule();
	}
	
	@Override
	public void postExecute() throws Exception {
		// Do nothing
	}
}
