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
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.erl.execute.RuleProfiler;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class DistributedEvlRunConfigurationSlave extends DistributedEvlRunConfiguration {

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
	 * @return A serializable representation of {@link RuleProfiler}.
	 */
	public HashMap<String, java.time.Duration> getSerializableRuleExecutionTimes() {
		return getModule().getContext()
			.getExecutorFactory().getRuleProfiler()
			.getExecutionTimes()
			.entrySet().stream()
			.collect(Collectors.toMap(
				e -> e.getKey().getName(),
				Map.Entry::getValue,
				(t1, t2) -> t1.plus(t2),
				HashMap::new
			));
	}
	
	@Override
	public EvlModuleDistributedSlave getModule() {
		return (EvlModuleDistributedSlave) super.getModule();
	}
	
	@Override
	protected EvlModuleDistributedSlave getDefaultModule() {
		return new EvlModuleDistributedSlave();
	}
	
	@Override
	public void postExecute() throws Exception {
		// Do nothing
	}
}
