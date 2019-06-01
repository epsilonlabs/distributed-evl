/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.jms.*;
import org.eclipse.epsilon.common.function.CheckedConsumer;
import org.eclipse.epsilon.common.function.CheckedRunnable;
import org.eclipse.epsilon.common.function.ExceptionContainer;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.data.DistributedEvlBatch;

/**
 * This module co-ordinates a message-based architecture. The workflow is as follows: <br/>
 * 
 * - Master is invoked in usual way, given the usual data (script, models etc.)
 *  + URI of the broker + expected number of workers + session ID <br/>
 *  
 * - Master waits on a registration queue for workers to confirm presence <br/>
 * 
 * - Master sends each worker their unique ID and the confirguration parameters
 * obtained from {@linkplain EvlContextDistributedMaster#getJobParameters()} <br/>
 * 
 * - Workers send back a message when they've loaded the configuration <br/>
 * 
 * - Jobs are sent to the workers (either as batches or individual model elements to evaluate) <br/>
 * 
 * - Workers send back results to results queue, which are then deserialized. <br/><br/>
 * 
 * The purpose of the {@linkplain #sessionID} is to prevent unauthorized workers from connecting, or messages being
 * received from different sessions. Every time a worker registers, they must provide the matching ID otherwise the
 * connection will be rejected.<br/>
 * 
 * Note that each worker is processed independently and asynchronously. That is, once a worker has connected,
 * it need not wait for other workers to connect or be in the same stage of registration. This module is
 * designed such that it is possible for a fast worker to start sending results back before another has even
 * registered. This class also tries to abstract away from the handshaking / messaging code by invoking methods
 * at key points in the back-and-forth messaging process within listeners which can be used to control the
 * execution strategy. See the {@link #checkConstraints()} method for where these "checkpoint" methods are.
 * <br/><br/>
 * 
 * It is the responsibility of subclasses to handled failed jobs sent from workers. The {@link #failedJobs}
 * collection gets appended to every time a failure message is received. This message will usually be the job that
 * was sent to the worker. Every time a failure is added, the collection object's monitor is notified.
 * Implementations can use this to listen for failures and take appropriate action, such as re-scheduling the jobs
 * or processing them directly. Although this handling can happen at any stage (e.g. either during execution or once
 * all workers have finished), the {@link #processFailedJobs(JMSContext)} method is guaranteed to be called after
 * {@linkplain #waitForWorkersToFinishJobs(AtomicInteger, JMSContext)} so any remaining jobs will be processed
 * if they have not been handled. This therefore requires that implementations should remove jobs if they process
 * them during execution to avoid unnecessary duplicate processing. <br/>
 * It should also be noted that the {@link #failedJobs} is not thread-safe, so manual synchronization is required.
 * 
 * @see EvlJmsWorker
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleJmsMaster extends EvlModuleDistributedMaster {
	
	public static final String
		JOBS_QUEUE = "worker_jobs",
		CONFIG_TOPIC = "configuration",
		END_JOBS_TOPIC = "no_more_jobs",
		STOP_TOPIC = "shortcircuit",
		REGISTRATION_QUEUE = "registration",
		RESULTS_QUEUE_NAME = "results",
		WORKER_ID_PREFIX = "EVL_jms",
		LAST_MESSAGE_PROPERTY = "lastMsg",
		WORKER_ID_PROPERTY = "workerID",
		CONFIG_HASH_PROPERTY = "configChecksum";
	
	protected final String host;
	protected final int sessionID;
	protected final int expectedSlaves;
	protected final Map<String, Map<String, Duration>> slaveWorkers;
	protected final Collection<Serializable> failedJobs;
	// Set this to false for unbounded scalability
	protected boolean refuseAdditionalWorkers = true;
	ConnectionFactory connectionFactory;
	private CheckedConsumer<Serializable, JMSException> jobSender;
	private CheckedRunnable<JMSException> completionSender;
	private Thread jobSenderThread;
	
	public EvlModuleJmsMaster(int expectedWorkers, String host, int sessionID) throws URISyntaxException {
		super(expectedWorkers);
		this.host = host;
		this.sessionID = sessionID;
		slaveWorkers = new java.util./*Hashtable*/concurrent.ConcurrentHashMap<>(
			this.expectedSlaves = expectedWorkers
		);
		failedJobs = new java.util.HashSet<>();
	}
	
	@Override
	protected void prepareExecution() throws EolRuntimeException {
		super.prepareExecution();
		connectionFactory = new org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory(host);
		log("Connected to "+host+" session "+sessionID);
	}
	
	@Override
	public List<DistributedEvlBatch> getBatches(double batchPercent) throws EolRuntimeException {
		return super.getBatches(batchPercent / Math.max(expectedSlaves, 1));
	}
	
	@Override
	protected final void checkConstraints() throws EolRuntimeException {
		try (JMSContext regContext = connectionFactory.createContext()) {
			// Initial registration of workers
			final Destination tempDest = regContext.createTemporaryQueue();
			final JMSProducer regProducer = regContext.createProducer();
			regProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			final Serializable config = getContext().getJobParameters(true);
			final int configHash = config.hashCode();
			final AtomicInteger registeredWorkers = new AtomicInteger();
			
			log("Awaiting workers");
			// Triggered when a worker announces itself to the registration queue
			regContext.createConsumer(createRegistrationQueue(regContext)).setMessageListener(msg -> {
				// For security / load purposes, stop additional workers from being picked up.
				if (refuseAdditionalWorkers && registeredWorkers.get() >= expectedSlaves) {
					return;
				}
				try {
					// Assign worker ID
					int currentWorkers = registeredWorkers.incrementAndGet();
					String workerID = createWorker(currentWorkers, msg);
					slaveWorkers.put(workerID, Collections.emptyMap());
					// Tell the worker what their ID is along with the configuration parameters
					Message configMsg = regContext.createObjectMessage(config);
					configMsg.setJMSReplyTo(tempDest);
					configMsg.setStringProperty(WORKER_ID_PROPERTY, workerID);
					configMsg.setIntProperty(CONFIG_HASH_PROPERTY, configHash);
					regProducer.send(msg.getJMSReplyTo(), configMsg);
				}
				catch (JMSException jmx) {
					throw new JMSRuntimeException(jmx.getMessage());
				}
				catch (NumberFormatException nan) {
					throw new java.lang.IllegalStateException("Worker registration failed!");
				}
			});
			
			try (JMSContext resultsContext = regContext.createContext(JMSContext.CLIENT_ACKNOWLEDGE)) {
				final AtomicInteger workersFinished = new AtomicInteger();
				
				resultsContext.createConsumer(createResultsQueue(resultsContext))
					.setMessageListener(getResultsMessageListener(workersFinished));
				
				final AtomicInteger readyWorkers = new AtomicInteger();
				// Triggered when a worker has completed loading the configuration
				regContext.createConsumer(tempDest).setMessageListener(response -> {
					try {
						final int receivedHash = response.getIntProperty(CONFIG_HASH_PROPERTY);
						if (receivedHash != configHash) {
							throw new java.lang.IllegalStateException("Received invalid configuration checksum!");
						}
						confirmWorker(response, readyWorkers);
					}
					catch (JMSException jmx) {
						throw new JMSRuntimeException(jmx.getMessage());
					}
				});
				
				try (JMSContext jobContext = resultsContext.createContext(JMSContext.CLIENT_ACKNOWLEDGE)) {
					final JMSProducer jobsProducer = jobContext.createProducer().setAsync(null);
					final Queue jobsQueue = createJobQueue(jobContext);
					jobSender = obj -> jobsProducer.send(jobsQueue, obj);
					final Topic completionTopic = createEndOfJobsTopic(jobContext);
					completionSender = () -> jobsProducer.send(completionTopic, jobContext.createMessage());
					
					processJobs(readyWorkers);
					if (jobSenderThread != null) jobSenderThread.join();
					waitForWorkersToFinishJobs(workersFinished);
					processFailedJobs();
				}
			}
		}
		catch (Exception ex) {
			try {
				stopAllWorkers(ex);
			}
			catch (JMSException jmx) {
				throw new JMSRuntimeException(
					"Couldn't stop workers! "+jmx.getMessage()+" (underlying: "+ex.getMessage()+")"
				);
			}
			if (ex instanceof RuntimeException) throw (RuntimeException) ex;
			if (ex instanceof EolRuntimeException) throw (EolRuntimeException) ex;
			if (ex instanceof JMSException) throw new JMSRuntimeException(ex.getMessage());
			else throw new EolRuntimeException(ex);
		}
	}
	
	/**
	 * Always called after execution, to finish unprocessed jobs. Implementations may override this
	 * method to handle the processing differently, e.g. to re-distributed failed jobs. Subclasses
	 * are free to call this method at any time prior to completion to avoid waiting.
	 * 
	 * @see #failedJobs
	 * @throws EolRuntimeException
	 */
	protected void processFailedJobs() throws EolRuntimeException {
		if (!failedJobs.isEmpty()) {
			log("Processing "+failedJobs.size()+" failed jobs...");
			for (Iterator<Serializable> it = failedJobs.iterator(); it.hasNext(); it.remove()) {
				executeJob(it.next());
			}
		}
	}
	
	/**
	 * 
	 * @param regContext The context to use for creating the Destination.
	 * @return The Destination used to listen for participating workers.
	 * @throws JMSException
	 */
	protected Queue createRegistrationQueue(JMSContext regContext) throws JMSException {
		return regContext.createQueue(REGISTRATION_QUEUE+sessionID);
	}
	
	/**
	 * 
	 * @param session The context to use for creating the Destination.
	 * @return The Destination used to listen for results in {@link #getResultsMessageListener(AtomicInteger)}.
	 * @throws JMSException
	 */
	protected Queue createResultsQueue(JMSContext session) throws JMSException {
		return session.createQueue(RESULTS_QUEUE_NAME+sessionID);
	}
	
	/**
	 * 
	 * @param session The inner-most JMSContext  from {@linkplain #checkConstraints()}.
	 * @return The Destination used to signal completion to workers when
	 * {@linkplain #signalCompletion()} is called.
	 * @throws JMSException
	 */
	protected Topic createEndOfJobsTopic(JMSContext session) throws JMSException {
		return session.createTopic(END_JOBS_TOPIC+sessionID);
	}
	
	/**
	 * 
	 * @param session The inner-most JMSContext  from {@linkplain #checkConstraints()}.
	 * @return The Destination for sending jobs to when {@link #sendJob(Serializable)} is called.
	 * @throws JMSException
	 */
	protected Queue createJobQueue(JMSContext session) throws JMSException {
		return session.createQueue(JOBS_QUEUE+sessionID);
	}
	
	/**
	 * 
	 * @param session
	 * @return The Destination for broadcasting that all workers should stop.
	 * @throws JMSException
	 */
	protected Topic createShortCircuitTopic(JMSContext session) throws JMSException {
		return session.createTopic(STOP_TOPIC+sessionID);
	}
	
	/**
	 * Sends the job to the job queue. This is a blocking call.
	 * 
	 * @param msgBody The workload (job)
	 * @throws JMSException
	 */
	protected final void sendJob(Serializable msgBody) throws JMSException {
		jobSender.acceptThrows(msgBody);
	}
	
	/**
	 * Broadcasts end of jobs to all workers.
	 * 
	 * @throws JMSException
	 */
	protected final void signalCompletion() throws JMSException {
		completionSender.runThrows();
	}

	/**
	 * Broadcasts to all workers to stop executing.
	 * 
	 * @param reason The message body to send to workers.
	 * @throws JMSException
	 */
	protected void stopAllWorkers(Exception exception) throws JMSException {
		try (JMSContext session = connectionFactory.createContext()) {
			session.createProducer().send(
				createShortCircuitTopic(session),
				exception.getMessage()
			);
		}
	}
	
	/**
	 * Main results processing listener. Implementations are expected to handle both results processing and
	 * signalling of terminal waiting condition once all workers have indicated all results have been
	 * processed. Due to the complexity of the implementation, it is not recommended that subclasses override
	 * this method. It is non-final for completeness / extensibility only. Incomplete / incorrect implementations
	 * will break the entire class, so overriding methods should be extremely careful and fully understand
	 * the inner workings / implementation of the base class if overriding this method.
	 * 
	 * @param workersFinished Mutable number of workers which have signalled completion status.
	 * @return A callback which can handle the semantics of results processing (i.e. deserialization and
	 * assignment) as well as co-ordination (signalling of completion etc.)
	 */
	protected MessageListener getResultsMessageListener(final AtomicInteger workersFinished) {
		final AtomicInteger resultsInProgress = new AtomicInteger();
		return msg -> {
			try {
				resultsInProgress.incrementAndGet();
				msg.acknowledge();
				
				if (msg.getBooleanProperty(LAST_MESSAGE_PROPERTY)) {
					String workerID = msg.getStringProperty(WORKER_ID_PROPERTY);
					if (!slaveWorkers.containsKey(workerID)) {
						throw new java.lang.IllegalStateException("Could not find worker with ID "+workerID);
					}
					
					workerCompleted(workerID, msg);
					
					if (workersFinished.incrementAndGet() >= expectedSlaves) {
						// Before signalling, we need to wait for all received results to be processed
						while (resultsInProgress.get() > 1) synchronized (resultsInProgress) {
							try {
								resultsInProgress.wait();
							}
							catch (InterruptedException ie) {}
						}
						synchronized (workersFinished) {
							workersFinished.notify();
						}
					}
				}
				else if (msg instanceof ObjectMessage) {
					Serializable contents = ((ObjectMessage)msg).getObject();
					if (contents instanceof Exception) {
						handleExceptionFromWorker((Exception) contents, msg.getStringProperty(WORKER_ID_PROPERTY));
					}
					else if (!deserializeResults(contents)) synchronized (failedJobs) {
						// Treat anything else (e.g. SerializableEvlInputAtom, DistributedEvlBatch) as a failure
						if (failedJobs.add(contents)) {
							failedJobs.notify();
						}
					}
				}
			}
			catch (JMSException jmx) {
				throw new JMSRuntimeException(jmx.getMessage());
			}
			catch (EolRuntimeException ex) {
				try {
					stopAllWorkers(ex);
				}
				catch (JMSException nested) {}
				throw new RuntimeException(ex.getMessage());
			}
			finally {
				if (resultsInProgress.decrementAndGet() <= 1 &&
					workersFinished.get() >= expectedSlaves)
						synchronized (resultsInProgress) {
							resultsInProgress.notify();
						}
			}
		};
	}
	
	/**
	 * This method is called in the body of {@link #checkConstraints()}, and is intended
	 * to be where the main processing logic goes. Immediately after this method, the
	 * {@link #waitForWorkersToFinishJobs(JMSContext)} is called.
	 * 
	 * @param readyWorkers  Convenience handle which may be used for synchronization, e.g.
	 * to call {@link #waitForWorkersToConnect(AtomicInteger)}.
	 * @throws Exception
	 */
	abstract protected void processJobs(final AtomicInteger workersReady) throws Exception;

	/**
	 * Convenience method for bulk sending of jobs followed by a call to {@link #signalCompletion()}.
	 * 
	 * @param jobs The Serializable jobs to send.
	 * @throws JMSException
	 */
	protected void sendAllJobs(Iterable<? extends Serializable> jobs) throws JMSException {
		for (Serializable job : jobs) {
			sendJob(job);
		}
		signalCompletion();
		log("Sent all jobs");
	}
	
	/**
	 * Sends all of the jobs in a new thread, returning immediately. If an exception is
	 * raised, no further jobs are submitted and the thread terminates. If all jobs
	 * were sent successfully, the {@link #signalCompletion()} method is called.
	 * 
	 * @param jobs The parameters to call {@link #sendJob(Serializable)} with.
	 * @return A handle on an exception, which will be present if sending any jobs failed.
	 */
	protected ExceptionContainer<JMSException> sendAllJobsAsync(Iterable<? extends Serializable> jobs) {
		ExceptionContainer<JMSException> exWrapper = new ExceptionContainer<>();
		
		jobSenderThread = new Thread(() -> {
			try {
				sendAllJobs(jobs);
			}
			catch (JMSException jmx) {
				exWrapper.setException(jmx);
				return;
			}
		});
		jobSenderThread.setName("job-sender");
		jobSenderThread.start();
		return exWrapper;
	}
	
	/**
	 * Called when a worker has registered.
	 * @param currentWorkers The number of currently registered workers.
	 * @param outbox The channel used to contact this work.
	 * @return The created worker.
	 */
	protected String createWorker(int currentWorkers, Message regMsg) {
		return WORKER_ID_PREFIX + currentWorkers;
	}

	/**
	 * Called when a worker has signalled its completion status. This method
	 * can be used to perform additional tasks.
	 * 
	 * @param worker The worker that has finished.
	 * @param msg The message received from the worker to signal this.
	 */
	@SuppressWarnings("unchecked")
	protected void workerCompleted(String worker, Message msg) throws JMSException {
		if (msg instanceof ObjectMessage) {
			Serializable body = ((ObjectMessage) msg).getObject();
			if (body instanceof Map) {
				slaveWorkers.put(worker, (Map<String, Duration>) body);
			}
		}
		log(worker + " finished");
	}

	/**
	 * Waits for the critical condition <code>workersFinished.get() >= expectedSlaves</code>
	 * to be signalled from the results processor as returned from {@linkplain #getResultsMessageListener()}.
	 * 
	 * @param workersFinished The number of workers that have signalled completion status. The value
	 * should not be mutated by this method, and only used for synchronising on the condition.
	 */
	protected void waitForWorkersToFinishJobs(AtomicInteger workersFinished) {
		log("Awaiting workers to signal completion...");
		while (workersFinished.get() < expectedSlaves) synchronized (workersFinished) {
			try {
				workersFinished.wait();
			}
			catch (InterruptedException ie) {}
		}
		log("All workers finished");
	}
	
	/**
	 * Called when a worker has completed loading its configuration. This method can be used to perform
	 * additional tasks, but should always call {@link WorkerView#confirm(JMSContext)}.
	 * 
	 * @param worker The worker that has been configured.
	 * @param workerReady The number of workers that have currently been configured, excluding this one.
	 * Implementations are expected to increment this number and can use the object's lock to signal
	 * when all workers are connected by comparing this number to {@linkplain #expectedSlaves}.
	 * @throws JMSException 
	 */
	protected void confirmWorker(final Message response, final AtomicInteger workersReady) throws JMSException {
		String worker = response.getStringProperty(WORKER_ID_PROPERTY);
		if (!slaveWorkers.containsKey(worker)) {
			throw new JMSRuntimeException("Could not find worker with id "+worker);
		}
		
		log(worker+" ready");
		
		if (workersReady.incrementAndGet() >= expectedSlaves) synchronized (workersReady) {
			workersReady.notify();
		}
	}
	
	/**
	 * Blocks until all expected workers have connected.
	 * 
	 * @param workersReady The number of currently connected workers.
	 */
	protected void waitForWorkersToConnect(AtomicInteger workersReady) {
		while (workersReady.get() < expectedSlaves) synchronized (workersReady) {
			try {
				workersReady.wait();
			}
			catch (InterruptedException ie) {}
		}
		assert workersReady.get() == expectedSlaves && slaveWorkers.size() == expectedSlaves;
		log("All workers connected");
	}
	
	/**
	 * Called when receiving a message with the {@link #EXCEPTION_PROPERTY}.
	 * 
	 * @param ex The received exception.
	 * @param workerID The received {@link #WORKER_ID_PROPERTY}.
	 */
	protected void handleExceptionFromWorker(Exception ex, String workerID) {
		log("Received exception "+ex.getMessage());
	}
	
	@Override
	protected void postExecution() throws EolRuntimeException {
		// Merge the workers' execution times with this one
		getContext().getExecutorFactory().getRuleProfiler().mergeExecutionTimes(
			slaveWorkers.values().stream()
				.flatMap(execTimes -> execTimes.entrySet().stream())
				.collect(Collectors.toMap(
					e -> this.constraints.stream()
						.filter(c -> c.getName().equals(e.getKey()))
						.findAny().get(),
					Map.Entry::getValue,
					(t1, t2) -> t1.plus(t2)
				))
		);
		
		super.postExecution();
		try {	
			teardown();
		}
		catch (Exception ex) {
			throw ex instanceof EolRuntimeException ? (EolRuntimeException) ex : new EolRuntimeException(ex);
		}
	}
	
	/**
	 * Cleanup method used to free resources once execution has completed.
	 * 
	 * @throws Exception
	 */
	protected void teardown() throws Exception {
		if (connectionFactory instanceof AutoCloseable) {
			((AutoCloseable) connectionFactory).close();
		}
	}
	
	/**
	 * Convenience method used for diagnostic purposes.
	 * 
	 * @param message The message to output.
	 */
	protected void log(Object message) {
		System.out.println("[MASTER] "+LocalTime.now()+" "+message);
	}
}
