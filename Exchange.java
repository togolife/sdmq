package sdmq;
import java.util.*;

public class Exchange {
	private Map<String, Map<String,MessageQueue>> queues = new HashMap<String, Map<String,MessageQueue>>();
	private String name;

	public Exchange(String name) {
		this.name = name;
	}
	
	public boolean addQueue(String key, String queueName, int dataDurable) {
		if (queues.containsKey(key)) {
			Map<String,MessageQueue> queueList = queues.get(key);
			if (!queueList.containsKey(queueName)) {
				MessageQueue mq = new MessageQueue(name, key, queueName, dataDurable);
				queueList.put(queueName, mq);
			}
		} else {
			MessageQueue mq = new MessageQueue(name, key, queueName, dataDurable);
			Map<String,MessageQueue> queueList = new HashMap<String,MessageQueue>();
			queueList.put(queueName, mq);
			queues.put(key, queueList);
		}
		return true;
	}
	
	public boolean addMessage(String key, String message) {
		if (queues.containsKey(key)) {
			Map<String,MessageQueue> queueList = queues.get(key);
			for (MessageQueue mq : queueList.values()) {
				mq.addMessage(message);
			}
		} else {
			return false;
		}
		return true;
	}
	
	public MessageQueue getQueue(String key, String queueName) {
		if (queues.containsKey(key)) {
			Map<String,MessageQueue> queueList = queues.get(key);
			return queueList.get(queueName);
		}
		return null;
	}
}
