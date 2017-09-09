package cn.gyyx.elves.scheduler.service.impl;

import cn.gyyx.elves.scheduler.service.ElvesSchedulerService;
import cn.gyyx.elves.scheduler.util.thrift.AgentService;
import cn.gyyx.elves.scheduler.util.thrift.Instruct;
import cn.gyyx.elves.scheduler.util.thrift.Reinstruct;
import cn.gyyx.elves.scheduler.util.thrift.SchedulerService;
import cn.gyyx.elves.util.ExceptionUtil;
import cn.gyyx.elves.util.SecurityUtil;
import cn.gyyx.elves.util.mq.MessageProducer;
import cn.gyyx.elves.util.mq.PropertyLoader;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: SchedulerServiceImpl
 * @Description: scheduler 服务实现类
 * @author East.F
 * @date 2016年11月8日 下午3:08:57
 */
@Component("elvesConsumerService")
public class SchedulerServiceImpl implements ElvesSchedulerService,SchedulerService.Iface{
	
	private static final Logger LOG=Logger.getLogger(SchedulerServiceImpl.class);
	
	@Autowired
	private MessageProducer producer;
	
	@Override
	public Map<String, Object> asyncJob(Map<String, Object> params){
		LOG.info("scheduler module reveive async job,params : "+params);
		
		String id = params.get("id").toString();
		String ip = params.get("ip")==null?"": params.get("ip").toString().trim();
		String mode = params.get("mode")==null?"":params.get("mode").toString().trim();
		String app = params.get("app")==null?"":params.get("app").toString().trim();
		String func = params.get("func")==null?"":params.get("func").toString().trim();
		String type = params.get("type")==null?"":params.get("type").toString();
		
		String param = params.get("param")==null?"":params.get("param").toString().trim();
		String timeout = params.get("timeout")==null?"":params.get("timeout").toString().trim();
		String proxy = params.get("proxy")==null?"":params.get("proxy").toString().trim();
		
		try {
			if(StringUtils.isBlank(id)||StringUtils.isBlank(ip)||StringUtils.isBlank(app)||StringUtils.isBlank(func)){
				return null;
			}
			
			if(!"cron".equalsIgnoreCase(type)&&!"queue".equalsIgnoreCase(type)){
				return null;
			}
			
			if(!"NP".equalsIgnoreCase(mode)&&!"P".equalsIgnoreCase(mode)){
				return null;
			}
			
			//默认90s
			int outTime = 90;
			if(StringUtils.isNotBlank(timeout)){
				outTime = Integer.parseInt(timeout);
			}
			
			List<Instruct> insList=new ArrayList<Instruct>();
			Instruct ins =new Instruct(id, ip, type, mode, app, func, param, outTime, proxy);
			insList.add(ins);
			
			List<Reinstruct> resultList=sendDataAsync(ip,insList);
			LOG.info("async job send to agent return data ："+resultList);
			if(null==resultList){
				//返回给发送方，任务发送错误   410.1
				Map<String,Object>  result= new HashMap<String,Object>();
				result.put("flag","false");
				result.put("error","[410.1]Send Job To Agent Fail");
                Map<String,Object>  rs= new HashMap<String,Object>();
                rs.put("id",id);
				result.put("result",rs);

				Map<String,Object>  data= new HashMap<String,Object>();
				String routingKey ="";
				String serverName="";
				if("cron".equalsIgnoreCase(type)){
					routingKey = "scheduler.cron";
					serverName = "cronResult";
				}else {
					routingKey = "scheduler.queue";
					serverName = "taskResult";
				}
				data.put("mqkey",routingKey+"."+serverName);
				data.put("mqtype","cast");
				data.put("mqbody",result);
				
				producer.cast(routingKey,JSON.toJSONString(data));
				LOG.info("asyncJob send to agent fail and send a msg to queue or cron,msg:"+JSON.toJSONString(data));
			}
		} catch (Exception e) {
			LOG.error("async job error : "+ExceptionUtil.getStackTraceAsString(e));
			return null;
		}
		return null;
	}
	
	@Override
	public Map<String, Object> syncJob(Map<String, Object> params){
		LOG.info("elves scheduler reveive sync job , params : "+params);
		Map<String,Object> rs=new HashMap<String,Object>();
		
		String id = SecurityUtil.getUniqueKey();
		
		String ip = params.get("ip")==null?"": params.get("ip").toString().trim();
		String mode = params.get("mode")==null?"":params.get("mode").toString().trim();
		String app = params.get("app")==null?"":params.get("app").toString().trim();
		String func = params.get("func")==null?"":params.get("func").toString().trim();
		String param = params.get("param")==null?"":params.get("param").toString().trim();
		String timeout = params.get("timeout")==null?"":params.get("timeout").toString().trim();
		String proxy = params.get("proxy")==null?"":params.get("proxy").toString().trim();
		
		//默认90s
		int outTime = 90;
		if(StringUtils.isNotBlank(timeout)){
			outTime = Integer.parseInt(timeout);
		}
		
		Instruct ins=new Instruct(id, ip, "rt", mode, app, func, param, outTime, proxy);
		Reinstruct back=sendDataSync(ip,outTime*1000+PropertyLoader.THRIFT_OUT_TIME, ins);
		LOG.info("sync job send to agent return data ："+back);
		if(null!=back){
			Map<String,Object> data=new HashMap<String,Object>();
			data.put("id",id);
			data.put("worker_flag",back.getFlag());
			data.put("worker_message",  back.getResult()==null?"":back.getResult());
			data.put("worker_costtime", back.getCosttime());
			
			rs.put("flag","true");
			rs.put("error","");
			rs.put("result",data);
		}else{
			rs.put("flag", "false");
			rs.put("error", "[410.0]Send Job To Agent Fail");
		}
		return rs;
	}
	
	@Override
	public String dataTransport(Reinstruct reins) throws TException {
		LOG.info("reveive agent back data reins:"+reins);
		
		Map<String,Object> rs=new HashMap<String,Object>();
		Map<String,Object>  data= new HashMap<String,Object>();
		rs.put("mqtype","cast");
		try {
			Map<String,Object> result=new HashMap<String,Object>();
			result.put("id",reins.getIns().getId());
			result.put("worker_flag",reins.getFlag());
			result.put("worker_message",reins.getResult());
			result.put("worker_costtime",reins.getCosttime());
			
			data.put("flag","true");
			data.put("error","");
			data.put("result",result);
			
			String routingKey ="";
			String serverName="";
			if("cron".equalsIgnoreCase(reins.getIns().getType())){
				routingKey = "scheduler.cron";
				serverName = "cronResult";
			}else if("queue".equalsIgnoreCase(reins.getIns().getType())){
				routingKey = "scheduler.queue";
				serverName = "taskResult";
			}else {
				//错误的type
				LOG.error("dataTransport reveive Reinstruct type error,type:"+reins.getIns().getType());
				return "success";
			}
			rs.put("mqkey",routingKey+"."+serverName);
			rs.put("mqbody",data);
			producer.cast(routingKey,JSON.toJSONString(rs));
		} catch (Exception e) {
			LOG.error("dataTransport error:"+ExceptionUtil.getStackTraceAsString(e));
		}
		return "success";
	}
	
	
	/**
	 * @Title: sendDataAsync
	 * @Description: asyncJob 发送agent
	 * @param ip
	 * @param insList
	 * @return List<Reinstruct>    返回类型
	 */
	public List<Reinstruct> sendDataAsync(String ip,List<Instruct> insList){
		TSocket transport=null;
		List<Reinstruct> resultList=null;
		try {
			transport = new TSocket(ip,PropertyLoader.THRIFT_AGENT_PORT);
			transport.setTimeout(PropertyLoader.THRIFT_OUT_TIME);
			TProtocol protocol = new TBinaryProtocol(transport);
			AgentService.Client client = new AgentService.Client(protocol);
			transport.open();
			resultList=client.instructionInvokeAsync(insList);
			LOG.info("异步指令转发到agent,返回结果："+resultList);
			return resultList;
		} catch (Exception e) {
			LOG.error("sync send data to agent error : "+ExceptionUtil.getStackTraceAsString(e));
			return null;
		} finally {
			if(null!=transport){
				transport.close();
			}
		}
	}
	
	/**
	 * @Title: sendDataSync
	 * @Description: syncJob 发送agent
	 * @param ip
	 * @param timeout
	 * @param ins
	 * @return Reinstruct    返回类型
	 */
	public Reinstruct sendDataSync(String ip,int timeout,Instruct ins){
		TSocket transport=null;
		Reinstruct back=null;
		try {
			transport = new TSocket(ip,PropertyLoader.THRIFT_AGENT_PORT);
			transport.setConnectTimeout(PropertyLoader.THRIFT_OUT_TIME);
			transport.setSocketTimeout(timeout);
			TProtocol protocol = new TBinaryProtocol(transport);
			AgentService.Client client = new AgentService.Client(protocol);
			transport.open();
			back=client.instructionInvokeSync(ins);
			return back;
		} catch (Exception e) {
			LOG.error("sync send data to agent error : "+ExceptionUtil.getStackTraceAsString(e));
			return null;
		} finally {
			if(null!=transport){
				transport.close();
			}
		}
	}
}
