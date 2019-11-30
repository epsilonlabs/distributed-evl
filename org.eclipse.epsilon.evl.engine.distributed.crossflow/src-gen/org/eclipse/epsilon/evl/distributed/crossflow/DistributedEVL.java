/** This class was automatically generated and should not be modified */
package org.eclipse.epsilon.evl.distributed.crossflow;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

import javax.annotation.Generated;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.eclipse.scava.crossflow.runtime.BuiltinStream;
import org.eclipse.scava.crossflow.runtime.Mode;
import org.eclipse.scava.crossflow.runtime.ParallelTaskList;
import org.eclipse.scava.crossflow.runtime.Workflow;
import org.eclipse.scava.crossflow.runtime.serialization.Serializer;
import org.eclipse.scava.crossflow.runtime.serialization.JsonSerializer;import org.eclipse.scava.crossflow.runtime.utils.ControlSignal;
import org.eclipse.scava.crossflow.runtime.utils.ControlSignal.ControlSignals;
import org.eclipse.scava.crossflow.runtime.utils.LogLevel;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

@Generated(value = "org.eclipse.scava.crossflow.java.Workflow2Class", date = "2019-11-30T17:04:27.022703400Z")
public class DistributedEVL extends Workflow<DistributedEVLTasks> {
	
	// Streams
	protected ValidationDataQueue validationDataQueue;
	protected ValidationOutput validationOutput;
	protected ConfigConfigTopic configConfigTopic;
	
	// Tasks
	protected ResultSink resultSink;
	protected ConfigConfigSource configConfigSource;
	protected ParallelTaskList<JobDistributor> jobDistributors = new ParallelTaskList<>();
	protected ParallelTaskList<Processing> processings = new ParallelTaskList<>();

	protected Set<DistributedEVLTasks> tasksToExclude = EnumSet.noneOf(DistributedEVLTasks.class);

	public DistributedEVL() {
		this(Mode.MASTER, 1);
	}
	
	public DistributedEVL(Mode m) {
		this(m, 1);
	}
	
	public DistributedEVL(Mode mode, int parallelization) {
		super();
			
		this.parallelization = parallelization;	
			
		this.name = "DistributedEVL";
		this.mode = mode;
		
		if (isMaster()) {
			for(int i=1;i<=parallelization;i++){
				JobDistributor task = new JobDistributor();
				task.setWorkflow(this);
				tasks.add(task);
				jobDistributors.add(task);
			}
			resultSink = new ResultSink();
			resultSink.setWorkflow(this);
			tasks.add(resultSink);
			configConfigSource = new ConfigConfigSource();
			configConfigSource.setWorkflow(this);
			tasks.add(configConfigSource);
		}
		
		if (isWorker()) {
			if (!tasksToExclude.contains(DistributedEVLTasks.PROCESSING)) {
				for(int i=1;i<=parallelization;i++){
					Processing task = new Processing();
					task.setWorkflow(this);
					tasks.add(task);
					processings.add(task);
				}
			}
		}
	}
	
	/**
	 * Run with initial delay in ms before starting execution (after creating broker
	 * if master)
	 * 
	 * @param delay
	 */
	@Override
	public void run(long delay) throws Exception {
		this.getSerializer();
		this.delay=delay;
	
		try {
						
			if (isMaster()) {
				if (createBroker) {
					if (activeMqConfig != null && activeMqConfig != "") {
						brokerService = BrokerFactory.createBroker(new URI("xbean:" + activeMqConfig));
					} else {
						brokerService = new BrokerService();
					}
				
					//activeMqConfig
					brokerService.setUseJmx(true);
					brokerService.addConnector(getBroker());
					if(enableStomp)
						brokerService.addConnector(getStompBroker());
					if(enableWS)	
						brokerService.addConnector(getWSBroker());
					brokerService.start();
				}
			}
	
			Thread.sleep(delay);
		
			jobDistributors.init(this);
			processings.init(this);
	
			validationDataQueue = new ValidationDataQueue(DistributedEVL.this, enablePrefetch);
			activeStreams.add(validationDataQueue);
			validationOutput = new ValidationOutput(DistributedEVL.this, enablePrefetch);
			activeStreams.add(validationOutput);
			configConfigTopic = new ConfigConfigTopic(DistributedEVL.this, enablePrefetch);
			activeStreams.add(configConfigTopic);
		
			if (isMaster()) {
					for(int i = 0; i <jobDistributors.size(); i++){
						JobDistributor task = jobDistributors.get(i);
						configConfigTopic.addConsumer(task, "JobDistributor");			
						task.setValidationDataQueue(validationDataQueue);
					}
					validationOutput.addConsumer(resultSink, "ResultSink");			
					configConfigSource.setConfigConfigTopic(configConfigTopic);
			}

			connect();
			
			if (isWorker()) {
				if (!tasksToExclude.contains(DistributedEVLTasks.PROCESSING)) {
						for(int i = 0; i <processings.size(); i++){
							Processing task = processings.get(i);
							validationDataQueue.addConsumer(task, "Processing");			
							configConfigTopic.addConsumer(task, "Processing");			
							task.setValidationOutput(validationOutput);
						}
				}
			}
			
			if (isMaster()){
				sendConfigurations();
			}
					
		} catch (Exception e) {
			log(LogLevel.ERROR, e.getMessage());
		}
	}				
	
	public void sendConfigurations(){
	
		new Thread(() -> {
					try {
						setTaskInProgess(configConfigSource);
						configConfigSource.produce();
						setTaskWaiting(configConfigSource);
					} catch (Exception ex) {
						reportInternalException(ex);
						terminate();
					}
		}).start();
	}
	
	public ValidationDataQueue getValidationDataQueue() {
		return validationDataQueue;
	}
	public ValidationOutput getValidationOutput() {
		return validationOutput;
	}
	public ConfigConfigTopic getConfigConfigTopic() {
		return configConfigTopic;
	}
	
	public JobDistributor getJobDistributor() {
		if(jobDistributors.size()>0)
			return jobDistributors.get(0);
		else 
			return null;
	}
	public ParallelTaskList<JobDistributor> getJobDistributors() {
		return jobDistributors;	
	}	
	public Processing getProcessing() {
		if(processings.size()>0)
			return processings.get(0);
		else 
			return null;
	}
	public ParallelTaskList<Processing> getProcessings() {
		return processings;	
	}	
	public ResultSink getResultSink() {
		return resultSink;
	}
	public ConfigConfigSource getConfigConfigSource() {
		return configConfigSource;
	}
	
	public DistributedEVL createWorker() {
		DistributedEVL worker = new DistributedEVL(Mode.WORKER,parallelization);
		worker.setInstanceId(instanceId);
		return worker;
	}
	
	@Override
	public DistributedEVL excludeTask(DistributedEVLTasks task) {
		if (task == null) throw new IllegalArgumentException("task cannot be null");
		this.tasksToExclude.add(task);
		return this;
	}
	
	@Override
	public DistributedEVL excludeTasks(Collection<DistributedEVLTasks> tasks) {
		for (DistributedEVLTasks t : tasks) {
			excludeTask(t);
		}
		return this;
	}
	
	@Override
	protected Serializer createSerializer() {
		return new JsonSerializer();
	}
	
	@Override
	protected void registerCustomSerializationTypes(Serializer serializer) {
		checkNotNull(serializer);
		serializer.registerType(Config.class);
		serializer.registerType(ValidationData.class);
		serializer.registerType(ValidationResult.class);
	}
	
	public static DistributedEVL run(String[] args) throws Exception {
		// Parse all values into an temporary object
		DistributedEVL argsHolder = new DistributedEVL();
		JCommander.newBuilder().addObject(argsHolder).build().parse(args);
		
		// Extract values to construct new object
		DistributedEVL app = new DistributedEVL(argsHolder.getMode(), argsHolder.getParallelization());
		JCommander.newBuilder().addObject(app).build().parse(args);
		app.run();
		return app;
	}
	
	public static void main(String[] args) throws Exception {
		// Parse all values into an temporary object
		DistributedEVL argsHolder = new DistributedEVL();
		JCommander jct = JCommander.newBuilder().addObject(argsHolder).build();
		
		try {
			jct.parse(args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		if(argsHolder.isHelp()) {
			jct.setProgramName("DistributedEVL");
			jct.usage();
			System.exit(0);
			}
			
		run(args);
	}
	
}	
