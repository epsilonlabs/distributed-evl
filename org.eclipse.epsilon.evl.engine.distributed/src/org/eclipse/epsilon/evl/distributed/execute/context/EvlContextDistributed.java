/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.execute.context;

import java.util.Set;
import org.eclipse.epsilon.common.module.IModule;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributed;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.context.IEvlContext;
import org.eclipse.epsilon.evl.execute.context.concurrent.EvlContextParallel;

/**
 * Note that {@link #isParallelisationLegal()} returns <code>false</code> by default
 * since the distribution framework may be doing parallelisation which is difficult to
 * detect. It's therefore recommended to override this behaviour if this is not the case.
 *
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextDistributed extends EvlContextParallel {
	
	protected static final String
		ENCODING = java.nio.charset.StandardCharsets.UTF_8.toString(),
		BASE_PATH = "basePath",
		BASE_PATH_SUBSTITUTE = "//BASEPATH//",
		LOCAL_PARALLELISM = "localParallelism",
		DISTRIBUTED_PARALLELISM = "distributedParallelism",
		EVL_SCRIPT = "evlScript",
		OUTPUT_DIR = "output",
		NUM_MODELS = "numberOfModels",
		MODEL_PREFIX = "model",
		SCRIPT_PARAMS = "scriptParameters",
		IGNORE_MODELS = "noModelLoading";
	
	public EvlContextDistributed(IEvlContext other) {
		super(other);
	}

	public EvlContextDistributed(int parallelism) {
		super(parallelism);
	}

	public void setUnsatisfiedConstraints(Set<UnsatisfiedConstraint> unsatisfiedConstraints) {
		this.unsatisfiedConstraints = unsatisfiedConstraints;
	}
	
	@Override
	public boolean isParallelisationLegal() {
		// TODO find a generic way to implement this
		return super.isParallelisationLegal();
	}
	
	@Override
	public void setModule(IModule module) {
		if (module instanceof EvlModuleDistributed) {
			super.setModule(module);
		}
	}
	
	@Override
	public EvlModuleDistributed getModule() {
		return (EvlModuleDistributed) super.getModule();
	}
}
