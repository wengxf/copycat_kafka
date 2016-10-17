package org.apache.kafka;

import java.lang.reflect.Method;

/**
 * 回调任务
 * @author baodekang
 *
 */
public class SchedulerTask<T> {

	private T instance;
	private String methodName;
	
	public SchedulerTask(T instance, String methodName) {
		this.instance = instance;
		this.methodName = methodName;
	}
	
	public String getMethodName() {
		return methodName;
	}
	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}
	
	public void invoke() throws Exception{
		Method method = instance.getClass().getDeclaredMethod(methodName);
		method.invoke(instance);
	}
}
