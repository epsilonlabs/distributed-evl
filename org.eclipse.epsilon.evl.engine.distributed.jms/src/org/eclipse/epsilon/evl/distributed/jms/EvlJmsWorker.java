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

import static org.eclipse.epsilon.evl.distributed.jms.EvlModuleJmsMaster.*;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalTime;
import java.util.Map;
import java.util.function.Consumer;
import javax.jms.*;
import org.eclipse.epsilon.common.function.CheckedRunnable;
import org.eclipse.epsilon.common.util.StringUtil;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.jms.internal.ConnectionFactoryObtainer;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationSlave;

/**
 * Reactive slave worker.
 * 
 * @see EvlModuleDistributedSlave
 * @see EvlModuleJmsMaster
 * @author Sina Madani
 * @since 1.6
 */
public final class EvlJmsWorker implements CheckedRunnable<Exception>, AutoCloseable {

	public static void main(String... args) throws Exception {
		if (args.length < 2) throw new java.lang.IllegalStateException(
			"Must provide base path and session ID!"
		);
		String basePath = args[0];
		int sessionID = Integer.valueOf(args[1]);
		
		String host = "tcp://localhost:61616";
		if (args.length > 2) try {
			host = args[2].contains("://") ? args[2] : "tcp://"+args[2];
			URI hostUri = new URI(host);
			host = hostUri.toString();
			if (hostUri.getPort() <= 0) {
				host += ":61616";
			}
		}
		catch (URISyntaxException urx) {
			System.err.println(urx);
			System.err.println("Using default "+host);
		}
		
		ConnectionFactory connectionFactory = ConnectionFactoryObtainer.get(host);
		try (EvlJmsWorker worker = new EvlJmsWorker(connectionFactory.createContext(), basePath, sessionID)) {
			System.out.println("Worker started for session "+sessionID);
			worker.run();
		}
		finally {
			if (connectionFactory instanceof AutoCloseable) {
				((AutoCloseable) connectionFactory).close();
			}
		}
	}
	
	final JMSContext jmsConnection;
	final String basePath;
	final int sessionID;
	DistributedEvlRunConfigurationSlave configContainer;
	Serializable stopBody;
	volatile boolean finished;
	int jobsProcessed = 0;
	Consumer<String> logger = System.out::println;
	String toStringCached;
	
	
	public EvlJmsWorker(JMSContext connection, String basePath, int sessionID) {
		this.jmsConnection = connection;
		this.basePath = basePath;
		this.sessionID = sessionID;
	}
	
	@Override
	public void runThrows() throws Exception {
		try (JMSContext regContext = jmsConnection.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
			Runnable ackSender = setup(regContext);
			
			try (JMSContext resultContext = jmsConnection.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
				try (JMSContext jobContext = jmsConnection.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
					try (JMSContext endContext = jmsConnection.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
						resultContext.createConsumer(resultContext.createTopic(STOP_TOPIC+sessionID)).setMessageListener(msg -> {
							log("Stopping execution!");
							try {
								stopBody = msg.getBody(Serializable.class);
							}
							catch (JMSException ex) {
								stopBody = ex;
							}
							finished = true;
						});
					
						endContext.createConsumer(endContext.createTopic(END_JOBS_TOPIC+sessionID)).setMessageListener(msg -> {
							try {
								msg.acknowledge();
							}
							catch (JMSException ex) {
								ex.printStackTrace();
							}
							log("Acknowledged end of jobs");
							finished = true;
						});
						
						JMSConsumer jobConsumer = jobContext.createConsumer(jobContext.createQueue(JOBS_QUEUE+sessionID));
						
						// Tell the master we're setup and ready to work. We need to send the message here
						// because if the master is fast we may receive jobs before we have even created the listener!
						ackSender.run();
						
						processJobs(jobConsumer, resultContext);
						onCompletion(resultContext);
					}
				}
			}
		}
	}
	
	Runnable setup(JMSContext regContext) throws Exception {
		// Announce our presence to the master
		Queue regQueue = regContext.createQueue(REGISTRATION_QUEUE+sessionID);
		JMSProducer regProducer = regContext.createProducer();
		regProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		Message initMsg = regContext.createMessage();
		Queue tempQueue = regContext.createTemporaryQueue();
		initMsg.setJMSReplyTo(tempQueue);
		regProducer.send(regQueue, initMsg);
		
		// Get the configuration and our ID
		Message configMsg = regContext.createConsumer(tempQueue).receive();
		Destination configAckDest = configMsg.getJMSReplyTo();
		Message configuredAckMsg = regContext.createMessage();
		Runnable ackSender = () -> regProducer.send(configAckDest, configuredAckMsg);
		
		try {
			log("Configuration received");
			
			@SuppressWarnings("unchecked")
			Map<String, ? extends Serializable> configMap = configMsg.getBody(Map.class);
			configContainer = DistributedEvlRunConfigurationSlave.parseJobParameters(configMap, basePath);
			logger = configContainer::writeOut;
			configContainer.preExecute();

			// This is to acknowledge when we have completed loading the script(s) and model(s) successfully
			configuredAckMsg.setIntProperty(CONFIG_HASH_PROPERTY, configMap.hashCode());
			return ackSender;
		}
		catch (Exception ex) {
			// Tell the master we failed
			ackSender.run();
			throw ex;
		}
	}
	
	/**
	 * Synchronous job processing.
	 * 
	 * @param jobConsumer
	 * @param replyContext
	 * @throws Exception
	 */
	void processJobs(JMSConsumer jobConsumer, JMSContext replyContext) throws Exception {
		JMSProducer resultSender = replyContext.createProducer().setAsync(null);
		Destination resultDest = replyContext.createQueue(RESULTS_QUEUE_NAME+sessionID);
		log("Began processing jobs");
		EvlContextDistributedSlave context = configContainer.getModule().getContext();
		
		Message msg;
		while ((msg = finished ? jobConsumer.receiveNoWait() : jobConsumer.receive()) != null) {
			try {
				msg.acknowledge();
				if (msg instanceof ObjectMessage)  {
					Serializable currentJob = ((ObjectMessage) msg).getObject();
					ObjectMessage resultsMsg = null;
					try {
						Serializable resultObj = (Serializable) context.executeJobStateless(currentJob);
						resultsMsg = replyContext.createObjectMessage(resultObj);
						++jobsProcessed;
					}
					catch (EolRuntimeException eox) {
						onFail(eox, msg);
						resultsMsg = replyContext.createObjectMessage(currentJob);
					}
					
					if (resultsMsg != null) {
						resultSender.send(resultDest, resultsMsg);
					}
				}
				
				if (msg.getBooleanProperty(LAST_MESSAGE_PROPERTY)) {
					finished = true;
				}
				else if (!(msg instanceof ObjectMessage)) {
					log("Received unexpected message of type "+msg.getClass().getName());
				}
			}
			catch (JMSException jmx) {
				onFail(jmx, msg);
				stopBody = jmx;
			}
			finally {
				if (stopBody != null) return;
			}
		}
		
		log("Finished processing jobs");
	}
	
	void onCompletion(JMSContext session) throws Exception {
		EvlContextDistributedSlave context = configContainer.getModule().getContext();
		// This is to ensure execution times are merged into main thread
		context.endParallelTask();
		
		ObjectMessage finishedMsg = session.createObjectMessage();
		finishedMsg.setBooleanProperty(LAST_MESSAGE_PROPERTY, true);
		finishedMsg.setIntProperty(NUM_JOBS_PROCESSED_PROPERTY, jobsProcessed);
		finishedMsg.setObject(stopBody != null ? stopBody : context.getSerializableRuleExecutionTimes());
		session.createProducer().send(session.createQueue(RESULTS_QUEUE_NAME+sessionID), finishedMsg);
		
		log("Signalled completion (processed "+jobsProcessed+" jobs)");
	}
	
	void onFail(Exception ex, Message msg) {
		log("Failed job '"+msg+"': "+ex);
	}

	void log(Object message) {
		logger.accept("["+this.toString()+"] "+LocalTime.now()+" "+message);
	}
	
	@Override
	public void close() throws Exception {
		if (jmsConnection instanceof AutoCloseable) {
			((AutoCloseable) jmsConnection).close();
		}
	}
	
	@Override
	public String toString() {
		if (StringUtil.isEmpty(toStringCached)) {
			toStringCached = getClass().getSimpleName()+"-"+hashCode()+" (session "+sessionID+")";
		}
		return toStringCached;
	}
}
