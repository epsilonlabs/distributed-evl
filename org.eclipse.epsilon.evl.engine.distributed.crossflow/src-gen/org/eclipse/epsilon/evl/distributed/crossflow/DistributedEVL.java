package org.eclipse.epsilon.evl.distributed.crossflow;

import java.util.List;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.eclipse.scava.crossflow.runtime.utils.ParallelTaskList;
import org.eclipse.scava.crossflow.runtime.utils.ControlSignal;
import org.eclipse.scava.crossflow.runtime.utils.ControlSignal.ControlSignals;
import org.eclipse.scava.crossflow.runtime.utils.CrossflowLogger.SEVERITY;
import org.eclipse.scava.crossflow.runtime.Workflow;
import org.eclipse.scava.crossflow.runtime.Mode;
import org.eclipse.scava.crossflow.runtime.BuiltinStream;

public class DistributedEVL extends Workflow {

	public static DistributedEVL run(String[] args) throws Exception {
		DistributedEVL throwAway = new DistributedEVL();
		new JCommander(throwAway, args);
		DistributedEVL app = new DistributedEVL(throwAway.getMode(),throwAway.getParallelization());
		new JCommander(app, args);
		app.run();
		return app;
	}
	
	public static void main(String[] args) throws Exception {
		run(args);
	}
	
	
	public DistributedEVL createWorker() {
		DistributedEVL worker = new DistributedEVL(Mode.WORKER,parallelization);
		worker.setInstanceId(instanceId);
		return worker;
	}
	
	
	// streams
	protected ValidationDataQueue validationDataQueue;
	protected ValidationOutput validationOutput;
	protected ConfigTopic configTopic;
	
	// tasks

	protected ResultSink resultSink;
	protected ConfigConfigSource configConfigSource;

	protected ParallelTaskList<JobDistributor> jobDistributors = new ParallelTaskList<>();
	protected ParallelTaskList<Processing> processings = new ParallelTaskList<>();

	//

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
			if (!tasksToExclude.contains("Processing")) {
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
		configTopic = new ConfigTopic(DistributedEVL.this, enablePrefetch);
		activeStreams.add(configTopic);
		
			if (isMaster()) {
					for(int i = 1; i <=jobDistributors.size(); i++){
						JobDistributor task = jobDistributors.get(i-1);
						task.setResultsTopic(resultsTopic);
						configTopic.addConsumer(task, "JobDistributor");			
						task.setValidationDataQueue(validationDataQueue);
					}
					resultSink.setResultsTopic(resultsTopic);
					validationOutput.addConsumer(resultSink, "ResultSink");			
					configConfigSource.setResultsTopic(resultsTopic);
					configConfigSource.setConfigTopic(configTopic);
			}
			
			if (isWorker()) {
				if (!tasksToExclude.contains("Processing")) {
						for(int i = 1; i <=processings.size(); i++){
							Processing task = processings.get(i-1);
							task.setResultsTopic(resultsTopic);
							validationDataQueue.addConsumer(task, "Processing");			
							configTopic.addConsumer(task, "Processing");			
							task.setValidationOutput(validationOutput);
						}
				}
			}
			
			if (isMaster()){
				// run all sources in parallel threads
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
	
	public ValidationDataQueue getValidationDataQueue() {
		return validationDataQueue;
	}
	public ValidationOutput getValidationOutput() {
		return validationOutput;
	}
	public ConfigTopic getConfigTopic() {
		return configTopic;
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
	
}	
