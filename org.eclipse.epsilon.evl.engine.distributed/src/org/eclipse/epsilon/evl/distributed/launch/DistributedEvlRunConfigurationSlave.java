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

import static org.eclipse.epsilon.eol.cli.EolConfigParser.parseModelParameters;
import static org.eclipse.epsilon.eol.cli.EolConfigParser.parseScriptParameters;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedSlave;

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
	
	public static Builder<? extends DistributedEvlRunConfigurationSlave, ?> Builder() {
		return new Builder<>(DistributedEvlRunConfigurationSlave.class);
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
	
	public static DistributedEvlRunConfigurationSlave parseJobParameters(Map<String, ? extends Serializable> config, String basePath) throws Exception {
		String masterBasePath, normBasePath;
		masterBasePath = Objects.toString(config.get(BASE_PATH), BASE_PATH_SUBSTITUTE);
		if (basePath != null) {
			normBasePath = basePath.replace("\\", "/");
			if (!normBasePath.endsWith("/")) {
				normBasePath += "/";
			}
		}
		else {
			normBasePath = Objects.toString(config.get(BASE_PATH));
			if (normBasePath == null) {
				normBasePath = Objects.toString(config.get(BASE_PATH_SUBSTITUTE));
			}
			if (normBasePath == null) {
				normBasePath = masterBasePath;
			}
		}
		
		String evlScriptPath = Objects.toString(config.get(EVL_SCRIPT), null);
		if (evlScriptPath == null) throw new IllegalStateException("No script path!");
		evlScriptPath = evlScriptPath.replace(masterBasePath, normBasePath).replace(normBasePath+"//", "/");
		
		Map<IModel, StringProperties> localModelsAndProperties;
		if (config.containsKey(IGNORE_MODELS)) {
			localModelsAndProperties = Collections.emptyMap();
		}
		else {
			int numModels = Integer.parseInt(Objects.toString(config.get(NUM_MODELS), "0"));
			String[] modelsConfig = new String[numModels];
			for (int i = 0; i < numModels; i++) {
				String modelConfig = Objects.toString(config.get(MODEL_PREFIX+i), null);
				if (modelConfig != null) {
					modelsConfig[i] = modelConfig
						.replace(masterBasePath, normBasePath)
						.replace(normBasePath+"//", "/")
						.replace("\\:", ":");
				}
			}
			localModelsAndProperties = parseModelParameters(modelsConfig);
		}
		
		Map<String, Object> scriptVariables = parseScriptParameters(
			Objects.toString(config.get(SCRIPT_PARAMS), "").split(",")
		);
		
		EvlModuleDistributedSlave localModule = new EvlModuleDistributedSlave(
			new EvlContextDistributedSlave(
				Integer.parseInt(Objects.toString(config.get(LOCAL_PARALLELISM), "0"))
			)
		);
		
		return new DistributedEvlRunConfigurationSlave(
			normBasePath,
			Paths.get(evlScriptPath),
			localModelsAndProperties,
			localModule,
			scriptVariables
		);
	}
	
	@Override
	public EvlModuleDistributedSlave getModule() {
		return (EvlModuleDistributedSlave) super.getModule();
	}
	
	@Override
	public void preExecute() throws Exception {
		super.preExecute();
		getModule().prepareExecution();
	}
	
	@Override
	public void postExecute() throws Exception {
		// Do nothing
	}
}
