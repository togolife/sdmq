package sdmq;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Calendar;

public class Log {
	private long fileSize = 0;
	private long currSize = 0;
	private String baseName;
	private String fileName;
	private String logPath = "./logs";
	
	public Log(String name, long size) {
		baseName = name;
		fileName = baseName + "." + fileSuffix();
		fileSize = size;
	}
	
	public Log(String logPath, String name, long size) {
		this(name, size);
		this.logPath = logPath;
	}
	
	// 输出日志
	public void print(String log) {
		StringBuilder sb = new StringBuilder();
		sb.append(getDateString(0));
		sb.append(' ');
		sb.append(log);
		sb.append('\n');
		
		try {
			File f = new File(logPath);
			if (!f.exists()) { // 判断日志目录是否存在，不存在创建目录
				f.mkdirs();
			}
			FileWriter writer = new FileWriter(logPath + "/" + fileName, true);
			writer.write(sb.toString());
			writer.close();
			currSize += sb.length();
			if (currSize >= fileSize) { // 日志较大，重新生成另外一个日志文件
				currSize = 0;
				fileName = baseName + "." + fileSuffix();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private String getDateString(int type) {
		Calendar d = Calendar.getInstance();
		StringBuilder sb = new StringBuilder();
		sb.append(d.get(Calendar.YEAR));
		if (d.get(Calendar.MONTH) < 10) {
			sb.append('0');
		}
		sb.append(d.get(Calendar.MONTH));
		if (d.get(Calendar.DAY_OF_MONTH) < 10) {
			sb.append('0');
		}
		sb.append(d.get(Calendar.DAY_OF_MONTH));
		if (type == 0)
			sb.append(' ');
		else
			sb.append('-');
		if (d.get(Calendar.HOUR_OF_DAY) < 10) {
			sb.append('0');
		}
		sb.append(d.get(Calendar.HOUR_OF_DAY));
		if (type == 0)
			sb.append(':');
		if (d.get(Calendar.MINUTE) < 10) {
			sb.append('0');
		}
		sb.append(d.get(Calendar.MINUTE));
		if (type == 0)
			sb.append(':');
		if (d.get(Calendar.SECOND) < 10) {
			sb.append('0');
		}
		sb.append(d.get(Calendar.SECOND));
		return sb.toString();
	}
	
	// 日期.pid
	private String fileSuffix() {
		StringBuilder sb = new StringBuilder();
		sb.append(getDateString(1));
		sb.append('.');
		// 获取进程PID
		RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
		sb.append(runtimeMXBean.getName().split("@")[0]);
		return sb.toString();
	}
}
