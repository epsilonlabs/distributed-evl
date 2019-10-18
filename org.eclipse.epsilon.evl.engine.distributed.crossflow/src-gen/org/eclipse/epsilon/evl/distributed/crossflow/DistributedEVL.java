/** This class was automatically generated and should not be modified */
package org.eclipse.epsilon.evl.distributed.crossflow;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Generated;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.eclipse.scava.crossflow.runtime.ParallelTaskList;
import org.eclipse.scava.crossflow.runtime.utils.ControlSignal;
import org.eclipse.scava.crossflow.runtime.utils.ControlSignal.ControlSignals;
import org.eclipse.scava.crossflow.runtime.utils.CrossflowLogger.SEVERITY;
import org.eclipse.scava.crossflow.runtime.Workflow;
import org.eclipse.scava.crossflow.runtime.Mode;
import org.eclipse.scava.crossflow.runtime.BuiltinStream;

@Generated(value = "org.eclipse.scava.crossflow.java.Workflow2Class", date = "2019-10-18T14:16:53.865523500+01:00[Europe/London]")
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
		
		// Register custom types and Job subclasses
		this.serializer.register(Config.class);
		this.serializer.register(ValidationData.class);
		this.serializer.register(ValidationResult.class);
	}
	
	/**
	 * Run with initial delay in ms before starting execution (after creating broker
	 * if master)
	 * 
	 * @param delay
	 */
	@Override
	public void run(long delay) throws Exception {
		jobDistributors.init(this);
		processings.init(this);
	
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
	
			connect();
	
			Thread.sleep(delay);
		
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
				
			// delay non-master connections to allow master to create the relevant listeners
			// (in a multi-threaded parallel execution) to facilitate termination, by
			// re-sending worker_added message
			if (!isMaster()) {
				Thread.sleep(1000);
				controlTopic.send(new ControlSignal(ControlSignals.WORKER_ADDED, getName()));
			}	
					
		} catch (Exception e) {
			log(SEVERITY.ERROR, e.getMessage());
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
	public DistributedEVL excludeTasks(EnumSet<DistributedEVLTasks> tasks) {
		for (DistributedEVLTasks t : tasks) {
			excludeTask(t);
		}
		return this;
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
