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
import org.eclipse.epsilon.common.concurrent.ConcurrencyUtils;
import org.eclipse.epsilon.common.module.IModule;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.erl.execute.data.JobBatch;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultPointer;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationSlave;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextDistributedSlave extends EvlContextDistributed {
	
	public EvlContextDistributedSlave() {
		super();
	}
	
	public EvlContextDistributedSlave(int localParallelism) {
		super(localParallelism);
	}
	
	@Override
	protected void initMainThreadStructures() {
		super.initMainThreadStructures();
		unsatisfiedConstraints = Collections.emptySet();
	}
	
	@Override
	public Set<UnsatisfiedConstraint> getUnsatisfiedConstraints() {
		return unsatisfiedConstraints;
	}
	
	boolean isBatchBased;
	
	@Override
	public Object executeJob(Object job) throws EolRuntimeException {
		if (job instanceof JobBatch) {
			isBatchBased = true;
		}
		else if (job instanceof SerializableEvlInputAtom) {
			isBatchBased = false;
		}
		
		if (job instanceof UnsatisfiedConstraint) {
			UnsatisfiedConstraint uc = (UnsatisfiedConstraint) job;
			return isBatchBased ?
				SerializableEvlResultPointer.serialize(uc, getModule()) :
				SerializableEvlResultAtom.serialize(uc, this);
		}
		
		Object result = super.executeJob(job);
		if (result instanceof UnsatisfiedConstraint) {
			result = executeJob(result);
		}
		return result;
	}
	
	/**
	 * Executes the provided Serializable job(s) and returns the Serializable result.
	 * The context's state (i.e. the UnsatisfiedConstraints) is not modified.
	 * 
	 * @param job The Serializable input job(s).
	 * @return A Serializable Collection containing zero or more {@link SerializableEvlResultAtom}s,
	 * or <code>null</code> if this module is the master.
	 * @throws EolRuntimeException If an exception occurs when executing the job using this module.
	 * @throws IllegalArgumentException If the job type was not recognised.
	 */
	@SuppressWarnings("unchecked")
	public Collection<? extends Serializable> executeJobStateless(Object job) throws EolRuntimeException {
		final Set<UnsatisfiedConstraint>
			originalUc = getUnsatisfiedConstraints(),
			tempUc = ConcurrencyUtils.concurrentSet(16, getParallelism());
		setUnsatisfiedConstraints(tempUc);
		
		try {
			executeJob(job);
			return (Collection<? extends Serializable>) executeJob(tempUc);
		}
		finally {
			setUnsatisfiedConstraints(originalUc);
		}
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
			normBasePath = masterBasePath;
		}
		
		String evlScriptPath = Objects.toString(config.get(EVL_SCRIPT), null);
		if (evlScriptPath == null) throw new IllegalStateException("No script path!");
		evlScriptPath = evlScriptPath.replace(masterBasePath, normBasePath);
		
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
					modelsConfig[i] = modelConfig.replace(masterBasePath, normBasePath);
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
	public void setModule(IModule module) {
		if (module instanceof EvlModuleDistributedSlave) {
			super.setModule(module);
		}
	}
}
