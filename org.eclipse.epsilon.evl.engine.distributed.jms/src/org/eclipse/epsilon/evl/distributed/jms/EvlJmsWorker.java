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
import java.util.Objects;
import javax.jms.*;
import org.eclipse.epsilon.common.function.CheckedRunnable;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedSlave;
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
			host = new URI(args[2]).toString();
		}
		catch (URISyntaxException urx) {
			System.err.println(urx);
			System.err.println("Using default "+host);
		}
		
		try (EvlJmsWorker worker = new EvlJmsWorker(host, basePath, sessionID)) {
			System.out.println("Worker started for session "+sessionID);
			worker.run();
		}
	}
	
	final ConnectionFactory connectionFactory;
	final String basePath;
	final int sessionID;
	boolean finished;
	String workerID;
	DistributedEvlRunConfigurationSlave configContainer;
	EvlModuleDistributedSlave module;
	Serializable stopBody;

	public EvlJmsWorker(String host, String basePath, int sessionID) {
		connectionFactory = new org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory(host);
		this.basePath = basePath;
		this.sessionID = sessionID;
	}
	
	@Override
	public void runThrows() throws Exception {
		try (JMSContext regContext = connectionFactory.createContext()) {
			Runnable ackSender = setup(regContext);
			
			try (JMSContext resultContext = regContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
				try (JMSContext jobContext = regContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
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
				
					jobContext.createConsumer(jobContext.createTopic(END_JOBS_TOPIC+sessionID)).setMessageListener(msg -> {
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
			this.workerID = configMsg.getStringProperty(WORKER_ID_PROPERTY);
			log("Configuration and ID received");
			configuredAckMsg.setStringProperty(WORKER_ID_PROPERTY, workerID);
			
			Map<String, ? extends Serializable> configMap = configMsg.getBody(Map.class);
			configContainer = EvlContextDistributedSlave.parseJobParameters(configMap, basePath);
			configContainer.preExecute();
			(module = (EvlModuleDistributedSlave) configContainer.getModule()).prepareExecution();

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
	
	void processJobs(JMSConsumer jobConsumer, JMSContext replyContext) throws Exception {
		JMSProducer resultSender = replyContext.createProducer().setAsync(null);
		Destination resultDest = replyContext.createQueue(RESULTS_QUEUE_NAME+sessionID);
		log("Began processing jobs");
		
		Message msg;
		while ((msg = finished ? jobConsumer.receiveNoWait() : jobConsumer.receive()) != null) try {
			if (msg instanceof ObjectMessage)  {
				Serializable currentJob = ((ObjectMessage) msg).getObject();
				ObjectMessage resultsMsg = null;
				try {
					Serializable resultObj = (Serializable) module.executeJob(currentJob);
					resultsMsg = replyContext.createObjectMessage(resultObj);
				}
				catch (EolRuntimeException eox) {
					onFail(eox, msg);
					resultsMsg = replyContext.createObjectMessage(currentJob);
				}
				
				if (resultsMsg != null) {
					resultsMsg.setStringProperty(WORKER_ID_PROPERTY, workerID);
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
	
	void onCompletion(JMSContext session) throws Exception {
		// This is to ensure execution times are merged into main thread
		module.getContext().endParallel();
		
		ObjectMessage finishedMsg = session.createObjectMessage();
		finishedMsg.setStringProperty(WORKER_ID_PROPERTY, workerID);
		finishedMsg.setBooleanProperty(LAST_MESSAGE_PROPERTY, true);
		finishedMsg.setObject(stopBody instanceof Serializable ? stopBody :
			configContainer.getSerializableRuleExecutionTimes()
		);
		session.createProducer().send(session.createQueue(RESULTS_QUEUE_NAME+sessionID), finishedMsg);
		
		log("Signalled completion");
	}
	
	void onFail(Exception ex, Message msg) {
		System.err.println("Failed job '"+msg+"': "+ex);
	}

	void log(Object message) {
		System.out.println("["+workerID+"] "+LocalTime.now()+" "+message);
	}
	
	@Override
	public void close() throws Exception {
		if (connectionFactory instanceof AutoCloseable) {
			((AutoCloseable) connectionFactory).close();
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!(obj instanceof EvlJmsWorker)) return false;
		EvlJmsWorker other = (EvlJmsWorker) obj;
		return
			Objects.equals(this.workerID, other.workerID) &&
			Objects.equals(this.sessionID, other.sessionID);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(workerID, sessionID);
	}
	
	@Override
	public String toString() {
		return getClass().getName()+"-"+workerID+" (session "+sessionID+")";
	}
}
