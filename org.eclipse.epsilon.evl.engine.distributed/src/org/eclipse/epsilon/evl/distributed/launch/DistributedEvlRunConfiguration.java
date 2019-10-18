/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.launch;

import static java.net.URLEncoder.encode;
import java.nio.file.Paths;
import java.util.Objects;
import org.eclipse.epsilon.common.util.OperatingSystem;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributed;
import org.eclipse.epsilon.evl.launch.EvlRunConfiguration;

/**
 * Run configuration container which holds the program arguments in the slave
 * nodes (i.e. the path to the script, the models, additional
 * parameters and arguments etc.).
 * 
 * @author Sina Madani
 * @since 1.6
 */
public abstract class DistributedEvlRunConfiguration extends EvlRunConfiguration {
	
	@SuppressWarnings("unchecked")
	public static class Builder<R extends DistributedEvlRunConfiguration, B extends Builder<R, B>> extends EvlRunConfiguration.Builder<R, B> {
		
		public String basePath, host;
		public int sessionID;
		
		public B withSessionID(int sid) {
			this.sessionID = sid;
			return (B) this;
		}
		public B withHost(String host) {
			this.host = host;
			return (B) this;
		}
		public B withBasePath(String base) {
			this.basePath = base;
			return (B) this;
		}
		
		protected void preBuild() {
			for (StringProperties props : modelsAndProperties.values()) {
				props.replaceAll((k, v) -> {
					// TODO better way to determine if there is a path?
					if (v instanceof String) {
						return appendBasePathIfNeeded(basePath, (String) v);
					}
					return v;
				});
			}
			if (script != null && !script.isAbsolute()) {
				script = Paths.get(basePath, script.toString());
			}
			if (outputFile != null && !outputFile.isAbsolute()) {
				outputFile = Paths.get(basePath, outputFile.toString());
			}
			System.setProperty(BASE_PATH_SYSTEM_PROPERTY, basePath);
		}
		
		protected R buildInstance() {
			return buildReflective(null);
		}
		
		@Override
		public final R build() {
			preBuild();
			return buildInstance();
		}
		
		protected Builder() {
			super();
		}
		protected Builder(Class<R> runConfigClass) {
			super(runConfigClass);
		}
	}
	
	protected static final String
		ENCODING = java.nio.charset.StandardCharsets.UTF_8.toString(),
		BASE_PATH = "basePath",
		LOCAL_PARALLELISM = "localParallelism",
		DISTRIBUTED_PARALLELISM = "distributedParallelism",
		EVL_SCRIPT = "evlScript",
		OUTPUT_DIR = "output",
		NUM_MODELS = "numberOfModels",
		MODEL_PREFIX = "model",
		SCRIPT_PARAMS = "scriptParameters",
		IGNORE_MODELS = "noModelLoading";
	
	public static final String
		BASE_PATH_SUBSTITUTE = "$BASEPATH$",
		BASE_PATH_SYSTEM_PROPERTY = "org.eclipse.epsilon.evl.distributed."+BASE_PATH;
	
	static String removeProtocol(String path) {
		String prot = "://";
		if (path != null && path.contains(prot)) {
			path = path.substring(path.indexOf(prot) + prot.length());
		}
		int driveIndex = path.indexOf(":/");
		if (OperatingSystem.isWindows() && driveIndex > 1) {
			path = path.substring(driveIndex - 1);
		}
		return path.replace("\\", "/");
	}
	
	static String appendBasePathIfNeeded(String basePath, String relPath) {
		return relPath.replace(BASE_PATH_SUBSTITUTE, removeProtocol(basePath));
	}
	
	public static Builder<? extends DistributedEvlRunConfiguration, ?> Builder() {
		return new Builder<>(DistributedEvlRunConfiguration.class);
	}
	
	protected final String host, basePath;
	protected final int sessionID;
	
	public DistributedEvlRunConfiguration(DistributedEvlRunConfiguration other) {
		super(other);
		this.basePath = other.basePath;
		this.host = other.host;
		this.sessionID = other.sessionID;
	}
	
	public DistributedEvlRunConfiguration(Builder<? extends DistributedEvlRunConfiguration, ?> builder) {
		super(builder);
		this.sessionID = builder.sessionID;
		this.basePath = builder.basePath;
		this.host = builder.host;
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
	
	@Override
	public EvlModuleDistributed getModule() {
		return (EvlModuleDistributed) super.getModule();
	}
}
