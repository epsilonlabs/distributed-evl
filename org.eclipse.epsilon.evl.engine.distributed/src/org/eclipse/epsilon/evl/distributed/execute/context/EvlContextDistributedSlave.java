/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.execute.context;

import static org.eclipse.epsilon.eol.cli.EolConfigParser.*;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.*;
import org.eclipse.epsilon.common.module.IModule;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationSlave;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextDistributedSlave extends EvlContextDistributed {
	
	protected static final Set<UnsatisfiedConstraint> NOOP_UC = new AbstractSet<UnsatisfiedConstraint>() {	
		@Override
		public boolean add(UnsatisfiedConstraint uc) {
			return true;
		}
		
		@Override
		public boolean addAll(Collection<? extends UnsatisfiedConstraint> c) {
			return false;
		}
		
		@Override
		public Iterator<UnsatisfiedConstraint> iterator() {
			throw new UnsupportedOperationException("This is a no-op collection!");
		}

		@Override
		public int size() {
			return 0;
		}
	};
	
	public EvlContextDistributedSlave(int localParallelism) {
		super(localParallelism);
	}
	
	@Override
	protected void initMainThreadStructures() {
		super.initMainThreadStructures();
		unsatisfiedConstraints = NOOP_UC;
	}
	
	@Override
	protected void initThreadLocals() {
		super.initThreadLocals();
		concurrentUnsatisfiedConstraints = null;
	}
	
	@Override
	public Set<UnsatisfiedConstraint> getUnsatisfiedConstraints() {
		return unsatisfiedConstraints;
	}
	
	public static DistributedEvlRunConfigurationSlave parseJobParameters(Map<String, ? extends Serializable> config, String basePath) throws Exception {
		String masterBasePath, normBasePath;
		masterBasePath = Objects.toString(config.get(BASE_PATH), null);
		boolean replaceBasePath = masterBasePath != null;
		if (basePath != null) {
			normBasePath = basePath.replace("\\", "/");
			if (!normBasePath.endsWith("/")) {
				normBasePath += "/";
			}
		}
		else {
			normBasePath = masterBasePath;
		}
		
		String evlScriptPath = Objects.toString(config.get(EVL_SCRIPT), null);
		if (evlScriptPath == null) throw new IllegalStateException("No script path!");
		if (replaceBasePath) evlScriptPath = evlScriptPath.replace(masterBasePath, normBasePath);
		
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
					modelsConfig[i] = replaceBasePath ? modelConfig.replace(masterBasePath, normBasePath) : modelConfig;
				}
			}
			localModelsAndProperties = parseModelParameters(modelsConfig);
		}
		
		Map<String, Object> scriptVariables = parseScriptParameters(
			Objects.toString(config.get(SCRIPT_PARAMS), "").split(",")
		);
		
		EvlModuleDistributedSlave localModule = new EvlModuleDistributedSlave(
			Integer.parseInt(Objects.toString(config.get(LOCAL_PARALLELISM), "0"))
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
	public void setModule(IModule module) {
		if (module instanceof EvlModuleDistributedSlave) {
			super.setModule(module);
		}
	}
}
