/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink;

import java.io.Serializable;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributed;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationSlave;

/**
 * Performs one-time setup on slave nodes. This mainly involves parsing the script,
 * loading models and putting variables into the FrameStack. Also takes care of
 * calling {@link EvlModuleDistributed#executeJob(Object)} and sending the results.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlFlinkFlatMapFunction<IN extends Serializable> extends RichFlatMapFunction<IN, SerializableEvlResultAtom> {
	
	private static final long serialVersionUID = 4605327252632042575L;
	
	protected transient EvlModuleDistributedSlave localModule;
	protected transient DistributedEvlRunConfigurationSlave configContainer;
	
	public static Configuration getParameters(RuntimeContext context, Configuration additionalParameters) {
		GlobalJobParameters globalParameters = context.getExecutionConfig().getGlobalJobParameters();
		Configuration parameters = null;
		if (globalParameters instanceof Configuration) {
			parameters = (Configuration) globalParameters;
		}
		else if (globalParameters instanceof ParameterTool) {
			parameters = ((ParameterTool)globalParameters).getConfiguration();
		}
		
		if (parameters == null || parameters.toMap().isEmpty()) {
			parameters = additionalParameters;
		}
		
		return parameters;
	}
	
	@Override
	public void flatMap(IN value, Collector<SerializableEvlResultAtom> out) throws Exception {
		localModule.executeJob(value).forEach(out::collect);
	}
	
	@Override
	public void open(Configuration additionalParameters) throws Exception {
		configContainer = EvlContextDistributedSlave.parseJobParameters(
			getParameters(getRuntimeContext(), additionalParameters).toMap(), null
		);
		localModule = configContainer.getModule();
		
		configContainer.preExecute();
		localModule.prepareExecution();
	}
}
