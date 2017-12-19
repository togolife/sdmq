package sdmq;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import java.util.*;
import java.util.Map.Entry;

class QueueProperty {
	public String exchange;
	public String key;
	public int dataDurable = 1;
}

public class Property {
	private String propertyFilePath = "./properties";
	private String propertyFileName = "mqserver.xml";
	
	// mq配置信息
	private Map<String, String> mq_properties = new HashMap<String, String>();
	// exchange配置信息
	private Set<String> exchange_properties = new HashSet<String>();
	// queue配置信息
	private Map<String, QueueProperty> queue_properties = new HashMap<String, QueueProperty>();
	
	public Property() {
		//
	}
	
	public Property(String path, String file) {
		this.propertyFilePath = path;
		this.propertyFileName = file;
	}
	
	public boolean load() {
		File f = new File(propertyFilePath + "/" + propertyFileName);
		if (!f.exists()) {
			return false;
		}
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		try {
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.parse(f);
			Element root = doc.getDocumentElement();
			NodeList children = root.getChildNodes();
			for (int i = 0; i < children.getLength(); ++i) {
				Node child = children.item(i);
				if (!(child instanceof Element)) {
					continue;
				}
				Element childElement = (Element)child;
				String tagName = childElement.getTagName();
				if (tagName.equals("exchange")) {
					NodeList exchangeChildren = childElement.getChildNodes();
					for (int j = 0; j < exchangeChildren.getLength(); ++j) {
						Node exchangeChild = exchangeChildren.item(j);
						if (!(exchangeChild instanceof Element)) {
							continue;
						}
						Element exchangeChildElement = (Element)exchangeChild;
						Text exchangeTextNode = (Text) exchangeChildElement.getFirstChild();
						String exchange_name = exchangeTextNode.getData().trim();
						exchange_properties.add(exchange_name);
					}
				} else if (tagName.equals("queue")) {
					String queue_name = null;
					QueueProperty queueProperty = new QueueProperty();
					NodeList queueChildren = childElement.getChildNodes();
					for (int j = 0; j < queueChildren.getLength(); ++j) {
						Node queueChild = queueChildren.item(j);
						if (!(queueChild instanceof Element)) {
							continue;
						}
						Element queueChildElement = (Element)queueChild;
						String queueTagName = queueChildElement.getTagName();
						Text queueTextNode = (Text) queueChildElement.getFirstChild();
						String value = queueTextNode.getData().trim();
						if (queueTagName.equals("name")) {
							queue_name = value;
						} else if (queueTagName.equals("exchange")) {
							queueProperty.exchange = value;
						} else if (queueTagName.equals("key")) {
							queueProperty.key = value;
						} else if (queueTagName.equals("data-durable")) {
							queueProperty.dataDurable = Integer.parseInt(value);
						}
					}
					if (queue_name != null) {
						queue_properties.put(queue_name, queueProperty);
					}
				} else {
					Text textNode = (Text) childElement.getFirstChild();
					String value = textNode.getData().trim();
					mq_properties.put(tagName, value);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public boolean addExchange(String name) {
		return exchange_properties.add(name);
	}
	
	public void addQueue(String name, String exchange, String key, int dataDurable) {
		QueueProperty queueProperty = new QueueProperty();
		queueProperty.exchange = exchange;
		queueProperty.key = key;
		queueProperty.dataDurable = dataDurable;
		queue_properties.put(name, queueProperty);
	}
	
	// 这里直接返回 private 成员
	public Map<String,String> getProperties() {
		return mq_properties;
	}
	
	public Set<String> getExchange() {
		return exchange_properties;
	}
	
	public Map<String, QueueProperty> getQueue() {
		return queue_properties;
	}

	public boolean save() {
		try {
			FileWriter fileWriter = new FileWriter(propertyFilePath + "/" + propertyFileName);
			fileWriter.write("<?xml version=\"1.0\"?>\n<mq>\n");
			for (Entry<String, String> m : mq_properties.entrySet()) {
				fileWriter.write("\t<" + m.getKey() + ">" + m.getValue() + "</" + m.getKey() + ">\n");
			}
			for (String e : exchange_properties) {
				fileWriter.write("\t<exchange>\n");
				fileWriter.write("\t\t<name>" + e + "</name>\n");
				fileWriter.write("\t</exchange>\n");
			}
			for (Entry<String, QueueProperty> q : queue_properties.entrySet()) {
				fileWriter.write("\t<queue>\n");
				fileWriter.write("\t\t<name>" + q.getKey() + "</name>\n");
				QueueProperty qp = q.getValue();
				fileWriter.write("\t\t<exchange>" + qp.exchange + "</exchange>\n");
				fileWriter.write("\t\t<key>" + qp.key + "</key>\n");
				fileWriter.write("\t\t<data-durable>" + qp.dataDurable + "</data-durable>\n");
				fileWriter.write("\t</queue>\n");
			}
			fileWriter.write("</mq>");
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static void main(String[] args) throws Exception {
		Property p = new Property();
		System.out.println(p.load());
		p.save();
	}

}
