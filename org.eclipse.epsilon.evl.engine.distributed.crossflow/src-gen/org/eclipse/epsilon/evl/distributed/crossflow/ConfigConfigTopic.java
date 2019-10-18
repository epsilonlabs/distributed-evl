/** This class was automatically generated and should not be modified */
package org.eclipse.epsilon.evl.distributed.crossflow;

import javax.annotation.Generated;
import javax.jms.*;
import org.apache.activemq.command.ActiveMQDestination;
import org.eclipse.scava.crossflow.runtime.Workflow;
import org.eclipse.scava.crossflow.runtime.Job;
import org.eclipse.scava.crossflow.runtime.JobStream;
import org.apache.activemq.command.ActiveMQBytesMessage;

@Generated(value = "org.eclipse.scava.crossflow.java.Steam2Class", date = "2019-10-18T14:16:53.865523500+01:00[Europe/London]")
public class ConfigConfigTopic extends JobStream<Config> {
		
	public ConfigConfigTopic(Workflow<DistributedEVLTasks> workflow, boolean enablePrefetch) throws Exception {
		super(workflow);
		
		ActiveMQDestination postQ;
			pre.put("JobDistributor", (ActiveMQDestination) session.createQueue("ConfigConfigTopicPre.JobDistributor." + workflow.getInstanceId()));
			destination.put("JobDistributor", (ActiveMQDestination) session.createQueue("ConfigConfigTopicDestination.JobDistributor." + workflow.getInstanceId()));
			postQ = (ActiveMQDestination) session.createTopic("ConfigConfigTopicPost.JobDistributor." + workflow.getInstanceId()
					+ (enablePrefetch?"":"?consumer.prefetchSize=1"));		
			post.put("JobDistributor", postQ);			
			pre.put("Processing", (ActiveMQDestination) session.createQueue("ConfigConfigTopicPre.Processing." + workflow.getInstanceId()));
			destination.put("Processing", (ActiveMQDestination) session.createQueue("ConfigConfigTopicDestination.Processing." + workflow.getInstanceId()));
			postQ = (ActiveMQDestination) session.createTopic("ConfigConfigTopicPost.Processing." + workflow.getInstanceId()
					+ (enablePrefetch?"":"?consumer.prefetchSize=1"));		
			post.put("Processing", postQ);			
		
		for (String consumerId : pre.keySet()) {
			ActiveMQDestination preQueue = pre.get(consumerId);
			ActiveMQDestination destQueue = destination.get(consumerId);
			ActiveMQDestination postQueue = post.get(consumerId);
			
			if (workflow.isMaster()) {
				MessageConsumer preConsumer = session.createConsumer(preQueue);
				consumers.add(preConsumer);
				preConsumer.setMessageListener(message -> {
					try {
						workflow.cancelTermination();
						Job job = (Job) workflow.getSerializer().toObject(getMessageText(message));
						
						if (workflow.getCache() != null && workflow.getCache().hasCachedOutputs(job)) {
							
							workflow.setTaskInProgess(cacheManagerTask);
							Iterable<Job> cachedOutputs = workflow.getCache().getCachedOutputs(job);
							workflow.setTaskWaiting(cacheManagerTask);
							
							for (Job output : cachedOutputs) {
								if (output.getDestination().equals("ValidationDataQueue")) {
									workflow.cancelTermination();
									((DistributedEVL) workflow).getValidationDataQueue().send((ValidationData) output, consumerId);
								}
								if (output.getDestination().equals("ValidationOutput")) {
									workflow.cancelTermination();
									((DistributedEVL) workflow).getValidationOutput().send((ValidationResult) output, consumerId);
								}
								
							}
						} else {
							MessageProducer producer = session.createProducer(destQueue);
							producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
							producer.send(message);
							producer.close();
						}
						
					} catch (Exception ex) {
						workflow.reportInternalException(ex);
					} finally { 
						try {
							message.acknowledge();
						} catch (Exception ex) {
							workflow.reportInternalException(ex);
						} 
					}				
				});
				
				MessageConsumer destinationConsumer = session.createConsumer(destQueue);
				consumers.add(destinationConsumer);
				destinationConsumer.setMessageListener(message -> {
					try {
						workflow.cancelTermination();
						Job job = (Job) workflow.getSerializer().toObject(getMessageText(message));
						
						if (workflow.getCache() != null && !job.isCached())
							if(job.isTransactional())
								workflow.getCache().cacheTransactionally(job);
							else
								workflow.getCache().cache(job);
						if(job.isTransactionSuccessMessage())
							return;
						MessageProducer producer = session.createProducer(postQueue);
						producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
						producer.send(message);
						producer.close();
					}
					catch (Exception ex) {
						workflow.reportInternalException(ex);
					} finally { 
						try {
							message.acknowledge();
						} catch (Exception ex) {
							workflow.reportInternalException(ex);
						} 
					}				
				});
			}
		}
	}
	
	public void addConsumer(ConfigConfigTopicConsumer consumer, String consumerId) throws Exception {
	
		ActiveMQDestination postQueue = post.get(consumerId);
		
		//only connect if the consumer exists (for example it will not in a master_bare situation)
		if (consumer != null) {		
			MessageConsumer messageConsumer = session.createConsumer(postQueue);
			consumers.add(messageConsumer);
			messageConsumer.setMessageListener(message -> {
				try {
					String messageText = getMessageText(message);
					Config config = (Config) workflow.getSerializer().toObject(messageText);
					consumer.consumeConfigConfigTopicWithNotifications(config);
				} catch (Exception ex) {
					workflow.reportInternalException(ex);
				} finally { 
					try {
						message.acknowledge();
					} catch (Exception ex) {
						workflow.reportInternalException(ex);
					} 
				}
			});
		}
	}
	
	private String getMessageText(Message message) throws Exception {
		if (message instanceof TextMessage) {
			return ((TextMessage) message).getText();
		}
		else if (message instanceof ActiveMQBytesMessage) {
			ActiveMQBytesMessage bm = (ActiveMQBytesMessage) message;
			byte data[] = new byte[(int) bm.getBodyLength()];
			bm.readBytes(data);
			return new String(data);
		}
		else return "";
	}
}

