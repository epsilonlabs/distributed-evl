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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import static java.net.URLDecoder.*;
import static java.net.URLEncoder.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.eclipse.epsilon.common.module.IModule;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.Variable;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.IEvlModule;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.data.LazyUnsatisfiedConstraint;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.strategy.JobSplitter;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextDistributedMaster extends EvlContextDistributed {

	protected Collection<StringProperties> modelProperties;
	protected Collection<Variable> initialVariables;
	protected int distributedParallelism;
	protected String outputDir, basePath;
	protected JobSplitter jobSplitter;
	
	public EvlContextDistributedMaster(int localParallelism, int distributedParallelism, JobSplitter splitter) {
		super(localParallelism);
		this.distributedParallelism = distributedParallelism;
		setJobSplitter(splitter);
	}
	
	protected void setJobSplitter(JobSplitter splitter) {
		this.jobSplitter = splitter != null ? splitter : new JobSplitter();
		splitter.setContext(this);
	}
	
	public JobSplitter getJobSplitter() {
		return jobSplitter;
	}
	
	@Override
	public void setUnsatisfiedConstraints(Set<UnsatisfiedConstraint> unsatisfiedConstraints) {
		this.unsatisfiedConstraints = unsatisfiedConstraints;
	}

	public void setModelProperties(Collection<StringProperties> modelProperties) {
		this.modelProperties = modelProperties;
	}
	
	public int getDistributedParallelism() {
		return distributedParallelism;
	}
	
	public void setDistributedParallelism(int parallelism) {
		this.distributedParallelism = parallelism;
	}
	
	public String getOutputPath() {
		return outputDir;
	}

	public void setOutputPath(String out) {
		this.outputDir = out;
	}
	
	// UnsatisfiedConstraint resolution
	
	@Override
	public Object executeJob(Object job) throws EolRuntimeException {
		if (job instanceof SerializableEvlResultAtom) {
			return getUnsatisfiedConstraints().add(((SerializableEvlResultAtom) job).deserializeLazy(getModule()));
		}
		return super.executeJob(job);
	}
	
	/**
	 * Resolves the serialized unsatisfied constraints lazily.
	 * 
	 * @param serializedResults The serialized UnsatisfiedConstraint instances.
	 * @return A Collection of lazily resolved UnsatisfiedConstraints.
	 */
	public Collection<LazyUnsatisfiedConstraint> deserializeLazy(Iterable<SerializableEvlResultAtom> serializedResults) {
		IEvlModule module = getModule();
		Collection<LazyUnsatisfiedConstraint> results = serializedResults instanceof Collection ?
			new ArrayList<>(((Collection<?>) serializedResults).size()) : new ArrayList<>();
		
		for (SerializableEvlResultAtom sr : serializedResults) {
			results.add(sr.deserializeLazy(module));
		}
		
		return results;
	}
	
	/**
	 * Deserializes the results eagerly in parallel using this context's ExecutorService.
	 * 
	 * @param results The serialized results.
	 * @param eager Whether to fully resolve each UnsatisfiedConstraint.
	 * @return The deserialized UnsatisfiedConstraints.
	 * @throws EolRuntimeException
	 */
	public Collection<UnsatisfiedConstraint> deserializeEager(Iterable<? extends SerializableEvlResultAtom> results) throws EolRuntimeException {
		IEvlModule module = getModule();
		ArrayList<Callable<UnsatisfiedConstraint>> jobs = results instanceof Collection ?
			new ArrayList<>(((Collection<?>)results).size()) : new ArrayList<>();
		
		for (SerializableEvlResultAtom sera : results) {
			jobs.add(() -> sera.deserializeEager(module));
		}
		
		return executeParallelTyped(null, jobs);
	}
	
	public void setBasePath(String path) {
		if (path != null) {
			try {
				this.basePath = decode(
					java.net.URI.create(encode(path, ENCODING)).normalize().toString(),
					ENCODING
				);
			}
			catch (IllegalArgumentException | UnsupportedEncodingException iax) {
				this.basePath = Paths.get(path).normalize().toString();
			}
		}
	}

	/**
	 * Saves the frame stack for the benefit of slave nodes.
	 */
	public void storeInitialVariables() {
		initialVariables = getFrameStack()
			.getFrames()
			.stream()
			.flatMap(frame -> frame.getAll().values().stream())
			//.filter(v -> StringUtil.isOneOf(v.getName(), "null", "System"))
			.collect(Collectors.toSet());
	}
	
	protected String removeBasePath(Object fullPath) {
		String fpStr = Objects.toString(fullPath);
		try {
			String fpNormal = fpStr
				.replace("\\", "/")
				.replace(
					java.net.URI.create(encode(basePath, ENCODING))
					.normalize().toString(), BASE_PATH_SUBSTITUTE
				)
				.replace(basePath.replace(" ", "%20"), BASE_PATH_SUBSTITUTE);
			
			return fpNormal.replace(basePath, BASE_PATH_SUBSTITUTE);
		}
		catch (Exception ex) {
			return fpStr;
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
	public HashMap<String, Serializable> getJobParameters(boolean stripBasePath) {
		HashMap<String, Serializable> config = new HashMap<>();
		
		if (stripBasePath) config.put(BASE_PATH, BASE_PATH_SUBSTITUTE);
		config.put(LOCAL_PARALLELISM, getParallelism());
		config.put(DISTRIBUTED_PARALLELISM, distributedParallelism);
		String scriptPath = getModule().getFile().toPath().toString();
		config.put(EVL_SCRIPT, stripBasePath ? removeBasePath(scriptPath) : scriptPath);
		config.put(OUTPUT_DIR, stripBasePath ? removeBasePath(outputDir) : outputDir);
		
		List<IModel> models = getModelRepository().getModels();
		int numModels = models.size();
		config.put(NUM_MODELS, numModels);
		
		if (modelProperties != null) {
			assert numModels == modelProperties.size();
			
			Iterator<StringProperties> modelPropertiesIter = modelProperties.iterator();
			
			for (int i = 0; i < numModels; i++) {
				config.put(MODEL_PREFIX+i,
					models.get(i).getClass().getName().replace("org.eclipse.epsilon.emc.", "")+"#"+
					modelPropertiesIter.next().entrySet().stream()
						.map(entry -> entry.getKey() + "=" + (stripBasePath ? removeBasePath(entry.getValue()) : entry.getValue()))
						.collect(Collectors.joining(","))
				);
			}
		}
		
		if (initialVariables != null) {
			String variablesFlattened = initialVariables
				.stream()
				.map(v -> v.getName() + "=" + Objects.toString(v.getValue()))
				.collect(Collectors.joining(","));
			
			config.put(SCRIPT_PARAMS, variablesFlattened);
		}
		
		return config;
	}
	
	@Override
	public EvlModuleDistributedMaster getModule() {
		return (EvlModuleDistributedMaster) super.getModule();
	}
	
	@Override
	public void setModule(IModule module) {
		if (module instanceof EvlModuleDistributedMaster) {
			super.setModule(module);
		}
	}
	
	public EvlContextDistributedMaster(EvlContextDistributedMaster other) {
		super(other);
		this.modelProperties = other.modelProperties;
		this.initialVariables = other.initialVariables;
		this.distributedParallelism = other.distributedParallelism;
		this.outputDir = other.outputDir;
		this.basePath = other.basePath;
		this.jobSplitter = other.jobSplitter;
	}
}
