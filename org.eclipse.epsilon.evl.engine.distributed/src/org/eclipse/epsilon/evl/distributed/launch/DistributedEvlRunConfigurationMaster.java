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

import static java.net.URLDecoder.decode;
import static java.net.URLEncoder.encode;
import static org.eclipse.epsilon.common.util.profiling.BenchmarkUtils.profileExecutionStage;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.file.Paths;
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
public abstract class DistributedEvlRunConfigurationMaster extends DistributedEvlRunConfiguration {

	@SuppressWarnings("unchecked")
	public static abstract class Builder<R extends DistributedEvlRunConfigurationMaster, B extends Builder<R, B>> extends DistributedEvlRunConfiguration.Builder<R, B> {
		protected static final double UNINTIALIZED_DOUBLE = -Double.MAX_VALUE;
		
		JobSplitter jobSplitter;
		public int distributedParallelism;
		public boolean shuffle = true;
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
	
	protected String normalBasePath;
	
	public DistributedEvlRunConfigurationMaster(DistributedEvlRunConfigurationMaster other) {
		super(other);
	}
	
	public DistributedEvlRunConfigurationMaster(Builder<? extends DistributedEvlRunConfiguration, ?> builder) {
		super(builder.skipModelLoading());
	}
	
	protected void setNormalBasePath(String path) {
		if (path != null) {
			try {
				normalBasePath = decode(
					java.net.URI.create(encode(path, ENCODING)).normalize().toString(),
					ENCODING
				);
			}
			catch (IllegalArgumentException | UnsupportedEncodingException iax) {
				normalBasePath = Paths.get(path).normalize().toString();
			}
		}
	}
	
	/**
	 * Converts the program's configuration into serializable key-value pairs which
	 * can then be used by slave modules to re-build an equivalent state. Such information
	 * includes the parallelism, path to the script, models and variables in the frame stack.
	 * 
	 * @param stripBasePath Whether to anonymise / normalise paths containing the basePath.
	 * @return The configuration properties.
	 */
	protected Serializable getJobParameters(boolean stripBasePath) {
		EvlContextDistributedMaster context = getModule().getContext();
		HashMap<String, Serializable> config = new HashMap<>();
		
		if (stripBasePath) config.put(BASE_PATH, BASE_PATH_SUBSTITUTE);
		if (context.isLocalParallelismExplicitlySpecified()) config.put(LOCAL_PARALLELISM, context.getParallelism());
		config.put(DISTRIBUTED_PARALLELISM, context.getDistributedParallelism());
		String scriptPath = getModule().getFile().toPath().toString();
		config.put(EVL_SCRIPT, stripBasePath ? removeBasePath(scriptPath) : scriptPath);
		if (outputFile != null) {
			String outDir = outputFile.toString();
			config.put(OUTPUT_DIR, stripBasePath ? removeBasePath(outDir) : outDir);
		}
		config.put(NUM_MODELS, modelsAndProperties.size());
			
		Iterator<Entry<IModel, StringProperties>> modelPropertiesIter = modelsAndProperties.entrySet().iterator();
		
		for (int i = 0; modelPropertiesIter.hasNext(); i++) {
			Entry<IModel, StringProperties> modelProp = modelPropertiesIter.next();
			config.put(MODEL_PREFIX+i,
				modelProp.getKey().getClass().getName().replace("org.eclipse.epsilon.emc.", "")+"#"+
				modelProp.getValue().entrySet().stream()
					.map(entry -> entry.getKey() + "=" + (stripBasePath ? removeBasePath(entry.getValue()) : entry.getValue()))
					.collect(Collectors.joining(","))
			);
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
		super.preExecute();
		CheckedRunnable<?> loadModels = this::loadModels, pw = this::prepareDistribution;
		ConcurrencyUtils.executeAsync(loadModels, pw);
	}
	
	protected void prepareDistribution() throws Exception {
		getModule().prepareExecution();
		if (getModule().getContext().getDistributedParallelism() == 0) return;
		if (profileExecution) {
			profileExecutionStage(profiledStages, "Sending configuration to workers",
				() -> getModule().prepareWorkers(getJobParameters(true))
			);
		}
		else {
			getModule().prepareWorkers(getJobParameters(true));
		}
	}
	
	@Override
	public EvlModuleDistributedMaster getModule() {
		return (EvlModuleDistributedMaster) super.getModule();
	}
}
