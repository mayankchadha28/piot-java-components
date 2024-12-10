/**
 * This class is part of the Programming the Internet of Things project.
 * 
 * It is provided as a simple shell to guide the student and assist with
 * implementation for the Programming the Internet of Things exercises,
 * and designed to be modified by the student as needed.
 */ 

package programmingtheiot.gda.connection;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SensorData;
import programmingtheiot.data.SystemPerformanceData;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Shell representation of class for student implementation.
 * 
 */
public class RedisPersistenceAdapter implements IPersistenceClient
{
	// static
	
	private static final Logger _Logger =
		Logger.getLogger(RedisPersistenceAdapter.class.getName());
	
	// private var's
	private String host = ConfigConst.DEFAULT_HOST;
	private int port = ConfigConst.DEFAULT_MQTT_PORT;
	
	private Jedis redisClient = null;
	
	// constructors
	
	/**
	 * Default.
	 * 
	 */
	public RedisPersistenceAdapter()
	{
		super();

		ConfigUtil configUtil = ConfigUtil.getInstance();

		this.host = configUtil.getProperty(ConfigConst.DATA_GATEWAY_SERVICE, 
			ConfigConst.HOST_KEY, ConfigConst.DEFAULT_HOST);

		this.port = configUtil.getInteger(
			ConfigConst.DATA_GATEWAY_SERVICE, ConfigConst.PORT_KEY
		);

		
		initConfig();
	}
	
	
	// public methods
	
	// public methods
	
	/**
	 *
	 */
	@Override
	public boolean connectClient()
	{
		try {
			if(this.redisClient == null){
				
				this.redisClient = new Jedis(host, port);
			}	

			if(! this.redisClient.isConnected()){
				_Logger.info("Redis client connecting to host:" + this.host);
				
				redisClient.connect();

				// _Logger.info("Redis client connected");
				
				return true;
				// String res = redisClient.ping();
				// if("PONG".equals(res)){
				// 	_Logger.info("Redis client connected");

				// 	return true;
				// }
				
				

			}else{
				_Logger.warning("Redis client already connected to host:" + this.host);
			}


		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to connect Redis client to host.", e);
		}

		return false;
	}

	/**
	 *
	 */
	@Override
	public boolean disconnectClient()
	{
		try {
			if(this.redisClient != null){
				
				if(this.redisClient.isConnected()){

					_Logger.info("Disconncting Redis client from host: "+ this.host);
					this.redisClient.disconnect();
					// _Logger.info("Redis client disconnected");
					return true;
				}else{
					_Logger.warning("Redis client not connected to host: "+ this.host);
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			_Logger.log(Level.SEVERE, "Failed to disconnect redis client from host: "
			+ this.host, e);
		}

		return false;
	}

	/**
	 *
	 */
	@Override
	public ActuatorData[] getActuatorData(String topic, Date startDate, Date endDate)
	{
		return null;
	}

	/**
	 *
	 */
	@Override
	public SensorData[] getSensorData(String topic, Date startDate, Date endDate)
	{
		return null;
	}

	/**
	 *
	 */
	@Override
	public void registerDataStorageListener(Class cType, IPersistenceListener listener, String... topics)
	{
	}

	/**
	 *
	 */
	@Override
	public boolean storeData(String topic, int qos, ActuatorData... data)
	{
		
		if(data != null && data.length > 0){
			
			_Logger.info("Storing Actuator Data to Redis Database");
			
			for(ActuatorData ad: data){
				String currentData = DataUtil.getInstance().actuatorDataToJson(ad);
				redisClient.lpush(topic, currentData);
			}

			return true;
		}

		return false;
	}

	/**
	 *
	 */
	@Override
	public boolean storeData(String topic, int qos, SensorData... data)
	{
		if(data != null && data.length > 0){
			
			_Logger.info("Storing Sensor Data to Redis Database");
			
			for(SensorData sd: data){
				String currentData = DataUtil.getInstance().sensorDataToJson(sd);
				redisClient.lpush(topic, currentData);
			}

			return true;
		}

		return false;
	}

	/**
	 *
	 */
	@Override
	public boolean storeData(String topic, int qos, SystemPerformanceData... data)
	{
		if(data != null && data.length > 0){
			
			_Logger.info("Storing System Performance Data to Redis Database");
			
			for(SystemPerformanceData sysPerfdata: data){
				String currentData = DataUtil.getInstance().systemPerformanceDataToJson(sysPerfdata);
				redisClient.lpush(topic, currentData);
			}

			return true;
		}

		return false;
	}
	
	
	// private methods
	
	/**
	 * 
	 */
	private void initConfig()
	{
	}

}
