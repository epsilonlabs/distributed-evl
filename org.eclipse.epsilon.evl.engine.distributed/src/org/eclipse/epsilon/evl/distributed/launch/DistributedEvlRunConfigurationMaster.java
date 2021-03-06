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

import static org.eclipse.epsilon.common.util.profiling.BenchmarkUtils.profileExecutionStage;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import org.eclipse.epsilon.common.concurrent.ConcurrencyUtils;
import org.eclipse.epsilon.common.function.CheckedRunnable;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.strategy.JobSplitter;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class DistributedEvlRunConfigurationMaster extends DistributedEvlRunConfiguration {

	@SuppressWarnings("unchecked")
	public static abstract class Builder<R extends DistributedEvlRunConfigurationMaster, B extends Builder<R, B>> extends DistributedEvlRunConfiguration.Builder<R, B> {
		protected static final double UNINTIALIZED_DOUBLE = -Double.MAX_VALUE;
		
		JobSplitter jobSplitter;
		public int distributedParallelism = Integer.MIN_VALUE;
		public boolean shuffle = true, localStandalone = false, stripBasePath = true;
		public double
			batchFactor = UNINTIALIZED_DOUBLE,
			masterProportion = UNINTIALIZED_DOUBLE;
		
		public B withDistributedParallelism(int workers) {
			this.distributedParallelism = workers;
			return (B) this;
		}
		public B withBatchFactor(double bf) {
			this.batchFactor = bf;
			return (B) this;
		}
		public B noShuffle() {
			return withShuffle(false);
		}
		public B withShuffle(boolean shuffle) {
			this.shuffle = shuffle;
			return (B) this;
		}
		public B withJobSplitter(JobSplitter splitter) {
			this.jobSplitter = splitter;
			return (B) this;
		}
		public B withLocalStandalone() {
			localStandalone = true;
			return (B) this;
		}
		public B rawBasePath() {
			return stripBaseBath(false);
		}
		public B stripBaseBath(boolean anon) {
			this.stripBasePath = anon;
			return (B) this;
		}
		
		@Override
		protected abstract EvlModuleDistributedMaster createModule();
		
		public JobSplitter getJobSplitter() {
			if (jobSplitter == null) {
				jobSplitter = new JobSplitter(shuffle, masterProportion, batchFactor);
			}
			return jobSplitter;
		}
		
		@Override
		protected void preBuild() {
			super.preBuild();
			getJobSplitter();
		}
		
		protected Builder() {
			super();
		}
		protected Builder(Class<R> runConfigClass) {
			super(runConfigClass);
		}
	}
	
	protected final boolean localStandalone;
	protected final boolean stripBasePath;
	
	public DistributedEvlRunConfigurationMaster(DistributedEvlRunConfigurationMaster other) {
		super(other);
		this.localStandalone = other.localStandalone;
		this.stripBasePath = other.stripBasePath;
	}
	
	public DistributedEvlRunConfigurationMaster(Builder<? extends DistributedEvlRunConfigurationMaster, ?> builder) {
		super(builder.skipModelLoading());
		this.localStandalone = builder.localStandalone;
		this.stripBasePath = builder.stripBasePath;
	}
	
	/**
	 * Converts the program's configuration into serializable key-value pairs which
	 * can then be used by slave modules to re-build an equivalent state. Such information
	 * includes the parallelism, path to the script, models and variables in the frame stack.
	 * 
	 * @return The configuration properties.
	 */
	protected Serializable getJobParameters() {
		EvlContextDistributedMaster context = getModule().getContext();
		HashMap<String, Serializable> config = new HashMap<>();
		
		config.put(BASE_PATH, stripBasePath ? BASE_PATH_SUBSTITUTE : this.basePath);
		if (context.isLocalParallelismExplicitlySpecified()) config.put(LOCAL_PARALLELISM, context.getParallelism());
		config.put(DISTRIBUTED_PARALLELISM, context.getDistributedParallelism());
		String scriptPath = getModule().getFile().toPath().toString();
		config.put(EVL_SCRIPT, stripBasePath ? removeBasePath(scriptPath) : scriptPath);
		if (outputFile != null) {
			String outDir = outputFile.toString();
			config.put(OUTPUT_DIR, stripBasePath ? removeBasePath(outDir) : outDir);
		}
		config.put(NUM_MODELS, modelsAndProperties != null ? modelsAndProperties.size() : 0);
		
		if (modelsAndProperties != null) {
			Iterator<Entry<IModel, StringProperties>> modelPropertiesIter = modelsAndProperties.entrySet().iterator();
			for (int i = 0; modelPropertiesIter.hasNext(); i++) {
				Entry<IModel, StringProperties> modelProp = modelPropertiesIter.next();
				if (modelProp == null) continue;
				IModel propKey = modelProp.getKey();
				if (propKey == null) continue;
				String key = propKey.getClass().getName().replace("org.eclipse.epsilon.emc.", "");
				StringProperties propValue = modelProp.getValue();
				String value = propValue != null ? propValue.entrySet().stream()
					.map(entry -> entry.getKey() + "=" + (stripBasePath ? removeBasePath(entry.getValue()) : entry.getValue()))
					.collect(Collectors.joining(",")) : null;
				
				config.put(MODEL_PREFIX+i, key+"#"+value);
			}
		}
		
		if (parameters != null) {
			String variablesFlattened = parameters.entrySet()
				.stream()
				.map(e -> e.getKey() + "=" + Objects.toString(e.getValue()))
				.collect(Collectors.joining(","));
			
			config.put(SCRIPT_PARAMS, variablesFlattened);
		}
		
		return config;
	}
	
	@Override
	public void preExecute() throws Exception {
		if (localStandalone) {
			createStandaloneWorkers();
		}
		super.preExecute();
		CheckedRunnable<?> pm = this::prepareMaster, pw = this::prepareWorkers;
		ConcurrencyUtils.executeAsync(pm, pw);
	}
	
	protected void createStandaloneWorkers() throws Exception {
		// Assume nothing needs to be done
		//throw new UnsupportedEncodingException();
	}

	protected void prepareMaster() throws Exception {
		loadModels();
		getModule().prepareExecution();
	}
	
	protected void prepareWorkers() throws Exception {
		EvlModuleDistributedMaster module = getModule();
		if (module.getContext().getDistributedParallelism() == 0) return;
		if (profileExecution) {
			profileExecutionStage(profiledStages, "Sending configuration to workers",
				() -> module.prepareWorkers(getJobParameters())
			);
		}
		else {
			module.prepareWorkers(getJobParameters());
		}
	}
	
	@Override
	public EvlModuleDistributedMaster getModule() {
		return (EvlModuleDistributedMaster) super.getModule();
	}
}
