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
import java.time.Duration;
import java.time.LocalTime;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.jms.*;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.control.ExecutionController;
import org.eclipse.epsilon.eol.execute.control.ExecutionProfiler;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.jms.execute.context.EvlContextJmsMaster;

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
 * It is the responsibility of subclasses to handle failed jobs sent from workers. The {@link #failedJobs}
 * collection gets appended to every time a failure message is received. This message will usually be the job that
 * was sent to the worker. Every time a failure is added, the collection object's monitor is notified.
 * Implementations can use this to listen for failures and take appropriate action, such as re-scheduling the jobs
 * or processing them directly. Although this handling can happen at any stage (e.g. either during execution or once
 * all workers have finished), the {@link #processFailedJobs(JMSContext)} method is guaranteed to be called after
 * {@linkplain #waitForWorkersToFinishJobs(AtomicInteger, JMSContext)} so any remaining jobs will be processed
 * if they have not been handled. This therefore requires that implementations should remove jobs if they process
 * them during execution to avoid unnecessary duplicate processing. <br/>
 * It should also be noted that the {@link #failedJobs} collection is not thread-safe, so manual synchronization is required.
 * 
 * @see EvlJmsWorker
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleJmsMaster extends EvlModuleDistributedMaster {
	
	public static final String
		JOBS_QUEUE = "worker_jobs",
		END_JOBS_TOPIC = "no_more_jobs",
		STOP_TOPIC = "shortcircuit",
		REGISTRATION_QUEUE = "registration",
		RESULTS_QUEUE_NAME = "results",
		LAST_MESSAGE_PROPERTY = "lastMsg",
		NUM_JOBS_PROCESSED_PROPERTY = "jobsProcessed",
		CONFIG_HASH_PROPERTY = "configChecksum";
	
	protected final Collection<Serializable> responses = new java.util.HashSet<>();
	JMSContext connectionContext;
	volatile int jobsProcessedByWorkers, jobsSentToWorkers;
	Consumer<String> logger = System.out::println;
	private final Object criticalConditionObj = this;
	
	public EvlModuleJmsMaster(EvlContextJmsMaster context) {
		super(context);
	}
	
	public void setLogger(Consumer<String> logger) {
		if (logger != null) this.logger = logger;
	}
	
	@Override
	protected void executeMasterJobs(Collection<?> jobs) throws EolRuntimeException {
		log("Total number of jobs = "+getAllJobs().size());
		super.executeMasterJobs(jobs);
		log("Finished processing "+jobs.size()+" master jobs");
	}
	
	@Override
	public final void prepareWorkers(Serializable configuration) throws EolRuntimeException {
		final EvlContextJmsMaster evlContext = getContext();
		final int sessionId = evlContext.getSessionId();
		try {
			connectionContext = evlContext.getConnectionFactory().createContext(JMSContext.AUTO_ACKNOWLEDGE);
				
			log("Connected to "+evlContext.getBrokerHost()+" session "+sessionId);
			// Initial registration of workers
			final Destination tempDest = connectionContext.createTemporaryQueue();
			final JMSProducer regProducer = connectionContext.createProducer();
			final int configHash = configuration.hashCode();
			final AtomicInteger workersReady = new AtomicInteger();
				
			// Triggered when a worker has completed loading the configuration
			connectionContext.createConsumer(tempDest).setMessageListener(response -> {
				try {
					response.acknowledge();
					final int receivedHash = response.getIntProperty(CONFIG_HASH_PROPERTY);
					if (receivedHash != configHash) {
						log(
							"Received invalid configuration checksum! Expected "+receivedHash+" but got "
							+ configHash + ". Discarding this worker."
						);
					}
					else {
						workersReady.incrementAndGet();
						synchronized (workersReady) {
							workersReady.notify();
						}
					}
				}
				catch (JMSException jmx) {
					log("Did not receive "+CONFIG_HASH_PROPERTY+": "+jmx.getMessage());
					jmx.printStackTrace();
				}
			});
				
			// Triggered when a worker announces itself to the registration queue
			connectionContext.createConsumer(connectionContext.createQueue(REGISTRATION_QUEUE + sessionId)).setMessageListener(msg -> {
				if (refuseWorker(workersReady.get())) return;
				
				try {
					Message configMsg = connectionContext.createObjectMessage(configuration);
					configMsg.setJMSReplyTo(tempDest);
					regProducer.send(msg.getJMSReplyTo(), configMsg);
				}
				catch (NumberFormatException | JMSException ex) {
					log("Worker registration failed - discarding this worker. Reason: "+ex.getMessage());
					ex.printStackTrace();
				}
			});
				
			beforeSend(workersReady);
		}
		catch (Exception ex) {
			handleException(ex);
		}
	}
	
	@Override
	protected final void executeWorkerJobs(Collection<? extends Serializable> jobs) throws EolRuntimeException {
		try (JMSContext resultContext = connectionContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
			
			resultContext.createConsumer(resultContext.createQueue(RESULTS_QUEUE_NAME + getContext().getSessionId()))
				.setMessageListener(getResultsMessageListener());
			
			try (JMSContext jobContext = resultContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {		
				final JMSProducer jobsProducer = jobContext.createProducer().setAsync(null);
				final Queue jobsQueue = jobContext.createQueue(JOBS_QUEUE + getContext().getSessionId());
				final Topic completionTopic = jobContext.createTopic(END_JOBS_TOPIC + getContext().getSessionId());
				
				for (Serializable job : jobs) {
					jobsProducer.send(jobsQueue, job);
				}
				jobsSentToWorkers += jobs.size();
				// Signal completion
				jobsProducer.send(completionTopic, jobContext.createMessage());
				
				log("Sent "+jobsSentToWorkers+" jobs to workers");
			}
			
			waitForWorkersToFinishJobs();
			processResponses(responses);
			responses.clear();
		}
		catch (Exception ex) {
			handleException(ex);
		}
	}
	
	private final void waitForWorkersToFinishJobs() {
		log("Awaiting workers to signal completion...");
		while (!getCriticalCondition()) synchronized (criticalConditionObj) {
			try {
				criticalConditionObj.wait();
			}
			catch (InterruptedException ie) {}
		}
		log("All workers finished ("+jobsProcessedByWorkers+" processed jobs)");
	}
	
	/**
	 * The criteria used to determine when workers have finished.
	 * 
	 * @return <code>true</code> if distributed processing is complete.
	 */
	private final boolean getCriticalCondition() {
		return jobsProcessedByWorkers >= jobsSentToWorkers;
	}
	
	/**
	 * Method used to limit additional worker registration.
	 * 
	 * @param workersReady The number of currently registered workers.
	 * @return <code>true</code> to ignore the worker registration.
	 */
	protected boolean refuseWorker(int workersReady) {
		return workersReady >= getContext().getDistributedParallelism();
	}
	
	protected void handleException(Exception ex) throws EolRuntimeException {
		stopAllWorkers(ex);
		if (ex instanceof RuntimeException) throw (RuntimeException) ex;
		if (ex instanceof EolRuntimeException) throw (EolRuntimeException) ex;
		if (ex instanceof JMSException) throw new JMSRuntimeException(ex.getMessage());
		else throw new EolRuntimeException(ex);
	}

	/**
	 * Called before {@link #sendAllJobs(Iterable)}. Used to wait
	 * for all workers to connect before proceeding.
	 * 
	 * @param workersReady Integer object indicating the number of workers that have
	 * successfully loaded the configuration and are ready to process jobs. The object's
	 * lock can be used to wait on some critical value to be reached.
	 */
	protected void beforeSend(AtomicInteger workersReady) throws Exception {
		log("Waiting for workers to load configuration...");
		int expected = Math.max(getContext().getDistributedParallelism(), 1);
		// Need to make sure someone is listening otherwise messages might be lost
		while (workersReady.get() < expected) synchronized (workersReady) {
			workersReady.wait();
		}
	}
	
	/**
	 * Always called after execution, to finish unprocessed jobs. Implementations may override this
	 * method to handle the processing differently, e.g. to re-distributed failed jobs. Subclasses
	 * are free to call this method at any time prior to completion to avoid waiting.
	 * Note that usually the jobs will be the results (i.e. SerializableEvlResultAtom).
	 * The processed responses will be removed.
	 * 
	 * @throws EolRuntimeException
	 */
	protected void processResponses(Collection<? extends Serializable> responseJobs) throws EolRuntimeException {
		if (!responseJobs.isEmpty()) {
			log("Processing "+responseJobs.size()+" repsonses...");
			getContext().executeJob(responseJobs);
		}
	}

	/**
	 * Broadcasts to all workers to stop executing.
	 * 
	 * @param reason The message body to send to workers.
	 * @throws JMSException
	 */
	protected void stopAllWorkers(Exception exception) throws JMSRuntimeException {
		try (JMSContext session = connectionContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
			session.createProducer().send(
				session.createTopic(STOP_TOPIC + getContext().getSessionId()),
				exception.getMessage()
			);
		}
	}
	
	/**
	 * Main results processing listener. Implementations are expected to handle both results processing and
	 * signalling of terminal waiting condition once all workers have indicated all results have been
	 * processed. Due to the complexity of the implementation, this method is non-overridable.
	 * 
	 * @return A callback which can handle the semantics of results processing (i.e. deserialization and
	 * assignment) as well as co-ordination (signalling of completion etc.)
	 */
	final MessageListener getResultsMessageListener() {
		final AtomicInteger resultsInProgress = new AtomicInteger();
		return msg -> {
			try {
				resultsInProgress.incrementAndGet();
				msg.acknowledge();
				
				if (msg.getBooleanProperty(LAST_MESSAGE_PROPERTY)) {
					
					workerCompleted(msg);
					if (getCriticalCondition()) {
						// Before signalling, we need to wait for all received results to be processed
						while (resultsInProgress.get() > 1) synchronized (resultsInProgress) {
							try {
								resultsInProgress.wait();
							}
							catch (InterruptedException ie) {}
						}
						synchronized (criticalConditionObj) {
							criticalConditionObj.notify();
						}
					}
				}
				else if (msg instanceof ObjectMessage) {
					Serializable contents = ((ObjectMessage)msg).getObject();
					if (contents instanceof Exception) {
						handleExceptionFromWorker((Exception) contents);
					}
					if (!processResponse(contents)) synchronized (responses) {
						responses.add(contents);
					}
				}
			}
			catch (JMSException jmx) {
				throw new JMSRuntimeException(jmx.getMessage());
			}
			catch (EolRuntimeException ex) {
				stopAllWorkers(ex);
				throw new RuntimeException(ex);
			}
			finally {
				if (resultsInProgress.decrementAndGet() <= 1 && getCriticalCondition()) synchronized (resultsInProgress) {
					resultsInProgress.notify();
				}
			}
		};
	}
	
	/**
	 * 
	 * @param response
	 * @return Whether the job was processed. Returning <code>true</code> will
	 * not add the response to the collection for post-processing. Returning <code>false</code>
	 * will deal with the job later once all other jobs have been executed.
	 * 
	 * @throws EolRuntimeException
	 */
	protected boolean processResponse(Serializable response) throws EolRuntimeException {
		getContext().executeJob(response);
		return true;
	}
	
	/**
	 * Called when a worker has signalled its completion status. This method
	 * can be used to perform additional tasks.
	 * 
	 * @param msg The message received from the worker to signal this.
	 */
	@SuppressWarnings("unchecked")
	protected void workerCompleted(Message msg) throws JMSException {
		int processed = msg.getIntProperty(NUM_JOBS_PROCESSED_PROPERTY);
		jobsProcessedByWorkers += processed;
		
		if (msg instanceof ObjectMessage) {
			Serializable body = ((ObjectMessage) msg).getObject();
			if (body instanceof Map) {
				// Merge the workers' execution times with this one
				ExecutionController controller = getContext().getExecutorFactory().getExecutionController();
				if (controller instanceof ExecutionProfiler) {
					((ExecutionProfiler) controller).mergeExecutionTimes(
						((Map<String, Duration>) body).entrySet().stream()
						.collect(Collectors.toMap(
							e -> this.constraints.stream()
								.filter(c -> c.getName().equals(e.getKey()))
								.findAny().get(),
							Map.Entry::getValue,
							(t1, t2) -> t1.plus(t2)
						))
					);
				}
			}
		}
		log("Worker finished (processed "+processed+" jobs)");
	}
	
	/**
	 * Called when receiving a message with the {@link #EXCEPTION_PROPERTY}.
	 * 
	 * @param ex The received exception.
	 * @param workerID The received {@link #WORKER_ID_PROPERTY}.
	 */
	protected void handleExceptionFromWorker(Exception ex) {
		log("Received exception "+ex.getMessage());
	}
	
	@Override
	protected void postExecution() throws EolRuntimeException {
		assert getCriticalCondition() && jobsProcessedByWorkers == jobsSentToWorkers : "All worker jobs processed";

		super.postExecution();
		try {
			if (connectionContext != null) {
				connectionContext.close();
			}
		}
		catch (Exception ex) {
			throw ex instanceof EolRuntimeException ? (EolRuntimeException) ex : new EolRuntimeException(ex);
		}
	}
	
	/**
	 * Convenience method used for diagnostic purposes.
	 * 
	 * @param message The message to output.
	 */
	protected void log(Object message) {
		logger.accept("[MASTER] "+LocalTime.now()+" "+message);
	}
	
	@Override
	public EvlContextJmsMaster getContext() {
		return (EvlContextJmsMaster) super.getContext();
	}
}
