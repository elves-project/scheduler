package cn.gyyx.elves.scheduler.service;

import java.util.Map;

/**
 * @ClassName: SchedulerService
 * @Description: scheduler提供的service服务
 * @author East.F
 * @date 2016年11月8日 下午3:07:44
 */
public interface ElvesSchedulerService {
	
	/**
	 * @Title: asyncJob
	 * @Description: 异步job任务
	 * @param params
	 * @return Map<String,Object>    返回类型
	 */
	public Map<String,Object> asyncJob(Map<String,Object> params);
	
	
	/**
	 * @Title: syncJob
	 * @Description: 同步job任务
	 * @param params
	 * @return Map<String,Object>    返回类型
	 */
	public Map<String,Object> syncJob(Map<String,Object> params);
	
	
}
