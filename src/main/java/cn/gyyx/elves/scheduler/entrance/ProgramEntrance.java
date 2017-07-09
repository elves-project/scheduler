package cn.gyyx.elves.scheduler.entrance;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import cn.gyyx.elves.scheduler.service.impl.SchedulerServiceImpl;
import cn.gyyx.elves.scheduler.util.thrift.SchedulerService;
import cn.gyyx.elves.util.ExceptionUtil;
import cn.gyyx.elves.util.SpringUtil;
import cn.gyyx.elves.util.mq.PropertyLoader;
import cn.gyyx.elves.util.zk.ZookeeperExcutor;

/**
 * @ClassName: ProgramEntrance
 * @Description: scheduler模块 程序入口
 * @author East.F
 * @date 2016年11月7日 上午9:29:31
 */
public class ProgramEntrance {

	private static final Logger LOG=Logger.getLogger(ProgramEntrance.class);
	
	
	/**
	 * 加载所有配置文件的路径
	 */
	private static void loadAllConfigFilePath(String configPath){
		SpringUtil.SPRING_CONFIG_PATH="file:"+configPath+File.separator+"conf"+File.separator+"spring.xml";
		SpringUtil.RABBITMQ_CONFIG_PATH="file:"+configPath+File.separator+"conf"+File.separator+"rabbitmq.xml";
		SpringUtil.PROPERTIES_CONFIG_PATH=configPath+File.separator+"conf"+File.separator+"conf.properties";
		SpringUtil.LOG4J_CONFIG_PATH=configPath+File.separator+"conf"+File.separator+"log4j.properties";
		LOG.info("load AllConfigFilePath success!");
	}
	
	/**
	 * 加载日志配置文件
	 */
	private static void loadLogConfig() throws Exception{
		InputStream in=new FileInputStream(SpringUtil.LOG4J_CONFIG_PATH);// 自定义配置
		PropertyConfigurator.configure(in);
		LOG.info("load LogConfig success!");
	}
	
	/**
	 * 加载Spring配置文件
	 */
	private static void loadApplicationXml() throws Exception{
		SpringUtil.app = new FileSystemXmlApplicationContext(SpringUtil.SPRING_CONFIG_PATH,SpringUtil.RABBITMQ_CONFIG_PATH);
		LOG.info("loadApplicationXml success!");
	}


	/**
	 * 注册模块到zk节点
	 */
	private static void registerZooKeeper() throws Exception{
		if("true".equalsIgnoreCase(PropertyLoader.ZOOKEEPER_ENABLED)){
			LOG.info("regist zookeeper ...."+PropertyLoader.ZOOKEEPER_HOST);
			ZookeeperExcutor zke=new ZookeeperExcutor(PropertyLoader.ZOOKEEPER_HOST,
					PropertyLoader.ZOOKEEPER_OUT_TIME, PropertyLoader.ZOOKEEPER_OUT_TIME);

			//创建模块根节点
			if(null==zke.getClient().checkExists().forPath(PropertyLoader.ZOOKEEPER_ROOT)){
				zke.getClient().create().creatingParentsIfNeeded().forPath(PropertyLoader.ZOOKEEPER_ROOT);
			}
			if(null==zke.getClient().checkExists().forPath(PropertyLoader.ZOOKEEPER_ROOT+"/scheduler")){
				zke.getClient().create().creatingParentsIfNeeded().forPath(PropertyLoader.ZOOKEEPER_ROOT+"/scheduler");
			}

			//创建节点
			String nodeName=zke.createNode(PropertyLoader.ZOOKEEPER_ROOT+"/scheduler/", "");
			if(null!=nodeName){
				//添加创建的节点监听，断线重连
				zke.addListener(PropertyLoader.ZOOKEEPER_ROOT+"/scheduler/", "");
			}
			LOG.info("registerZooKeeper success!");
		}
	}
	
	/**
	 * @Title: startSchedulerThriftService
	 * @Description: 开启scheduler 同步、异步调用服务
	 * @return void 返回类型
	 */
	private static void startSchedulerThriftService() {
		LOG.info("start scheduler thrift service....");
		new Thread() {
			@Override
			public void run(){
				try {
					SchedulerServiceImpl schedulerServiceImpl =  SpringUtil.getBean(SchedulerServiceImpl.class); 
					TProcessor tprocessor = new SchedulerService.Processor<SchedulerService.Iface>(schedulerServiceImpl);
					TServerSocket serverTransport = new TServerSocket(PropertyLoader.THRIFT_SCHEDULER_PORT);
					TThreadPoolServer.Args ttpsArgs = new TThreadPoolServer.Args(serverTransport);
					ttpsArgs.processor(tprocessor);
					ttpsArgs.protocolFactory(new TBinaryProtocol.Factory());
					//线程池服务模型，使用标准的阻塞式IO，预先创建一组线程处理请求。
					TServer server = new TThreadPoolServer(ttpsArgs);
					server.serve();
				} catch (Exception e) {
					LOG.error("start scheduler thrift service error,msg:"+ExceptionUtil.getStackTraceAsString(e));
				}
			}
		}.start();
		LOG.info("start scheduler thrift server success!");
	}
	
	public static void main(String[] args) {
		//args =new String[]{"E:\\jar-test\\scheduler"};
		if(null!=args&&args.length>0){
			try {
				loadAllConfigFilePath(args[0]);

		    	loadLogConfig();

				loadApplicationXml();

				registerZooKeeper();

				startSchedulerThriftService();
			} catch (Exception e) {
				LOG.error("start scheduler error:"+ExceptionUtil.getStackTraceAsString(e));
				System.exit(1);
			}
    	}
		
	}
}
