/**
 * This class is part of the Programming the Internet of Things project.
 * 
 * It is provided as a simple shell to guide the student and assist with
 * implementation for the Programming the Internet of Things exercises,
 * and designed to be modified by the student as needed.
 */ 

package programmingtheiot.gda.app;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.pool.ConnFactory;
import org.eclipse.californium.core.config.CoapConfig;

import com.amazonaws.auth.policy.Resource;
import com.influxdb.client.domain.Config;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.IActuatorDataListener;
import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;

import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.BaseIotData;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SensorData;
import programmingtheiot.data.SystemPerformanceData;

import programmingtheiot.gda.connection.CloudClientConnector;
import programmingtheiot.gda.connection.CoapServerGateway;
import programmingtheiot.gda.connection.IPersistenceClient;
import programmingtheiot.gda.connection.IPubSubClient;
import programmingtheiot.gda.connection.IRequestResponseClient;
import programmingtheiot.gda.connection.MqttClientConnector;
import programmingtheiot.gda.connection.RedisPersistenceAdapter;
import programmingtheiot.gda.connection.SmtpClientConnector;
import programmingtheiot.gda.system.SystemPerformanceManager;

/**
 * Shell representation of class for student implementation.
 *
 */
public class DeviceDataManager implements IDataMessageListener
{
	// static
	
	private static final Logger _Logger =
		Logger.getLogger(DeviceDataManager.class.getName());
	
	private boolean enableMqttClient = true;
	private boolean enableCoapServer = false;
	private boolean enableCloudClient = false;
	private boolean enableSmtpClient = false;
	private boolean enablePersistenceClient = false;
	private boolean enableSystemPerf = false;
	
	private IActuatorDataListener actuatorDataListener = null;
	private IPubSubClient mqttClient = null;
	private IPubSubClient cloudClient = null;
	private IPersistenceClient persistenceClient = null;
	private IRequestResponseClient smtpClient = null;
	private CoapServerGateway coapServer = null;
	private SystemPerformanceManager sysPerfMgr = null;

	private ActuatorData latestHumidifierActuatorData = null;
	private ActuatorData latestHumidifierActuatorResponse = null;
	private SensorData latestHumiditySensorData = null;
	private OffsetDateTime latestHumiditySensorTimeStamp = null;

	private boolean handleHumidityChangeOnDevice = false;
	private int lastKnownHumidifierCommand = ConfigConst.OFF_COMMAND;

	// Load from PiotConfig.props
	private long humidityMaxTimePastThreshold = 300;
	private float nominalHumiditySetting = 40.0f;
	private float triggerHumidifierFloor = 30.0f;
	private float triggerHumidifierCeiling = 50.0f;

	
	// constructors
	
	public DeviceDataManager()
	{
		super();

		ConfigUtil configUtil = ConfigUtil.getInstance();

		this.enableMqttClient = configUtil.getBoolean(
			ConfigConst.GATEWAY_DEVICE, ConfigConst.ENABLE_MQTT_CLIENT_KEY);
		
		this.enableCoapServer = configUtil.getBoolean(
			ConfigConst.GATEWAY_DEVICE, ConfigConst.ENABLE_COAP_SERVER_KEY);

			_Logger.warning("CoAP Status is: "+ this.enableCoapServer);

		this.enableCloudClient = configUtil.getBoolean(
			ConfigConst.GATEWAY_DEVICE, ConfigConst.ENABLE_CLOUD_CLIENT_KEY);

		this.enablePersistenceClient = configUtil.getBoolean(
			ConfigConst.GATEWAY_DEVICE, ConfigConst.ENABLE_PERSISTENCE_CLIENT_KEY);

		this.handleHumidityChangeOnDevice = configUtil.getBoolean(
			ConfigConst.GATEWAY_DEVICE, "handleHumidityChangeOnDevice");

		this.humidityMaxTimePastThreshold = 
			configUtil.getInteger(ConfigConst.GATEWAY_DEVICE, "humidityMaxTimePastThreshold");

		this.nominalHumiditySetting = 
			configUtil.getFloat(
				ConfigConst.GATEWAY_DEVICE, "nominalHumiditySetting");

		this.triggerHumidifierFloor = 
			configUtil.getFloat(
				ConfigConst.GATEWAY_DEVICE, "triggerHumidifierFloor");

		this.triggerHumidifierCeiling = 
			configUtil.getFloat(ConfigConst.GATEWAY_DEVICE, "triggerHumidifierCeiling");

		if(this.humidityMaxTimePastThreshold < 10 || this.humidityMaxTimePastThreshold > 7200){
			this.humidityMaxTimePastThreshold = 10;
		}
		
		initConnections();
	}
	
	public DeviceDataManager(
		boolean enableMqttClient,
		boolean enableCoapClient,
		boolean enableCloudClient,
		boolean enableSmtpClient,
		boolean enablePersistenceClient)
	{
		super();
		
		initConnections();
	}
	
	
	// public methods
	
	@Override
	public boolean handleActuatorCommandResponse(ResourceNameEnum resourceName, ActuatorData data)
	{
		if(data != null){
			_Logger.info("Handling actuator response: " + data.getName());

			if(data.hasError()){
				_Logger.warning("Error flag set for ActuatorData instance.");
			}

			return true;
		
		}else{
			return false;
		}
	}

	@Override
	public boolean handleActuatorCommandRequest(ResourceNameEnum resourceName, ActuatorData data)
	{
		return false;
	}

	@Override
	public boolean handleIncomingMessage(ResourceNameEnum resourceName, String msg)
	{
		if(msg != null){
			_Logger.info("Handling incoming generic message: " + msg);

			return true;
		}else{
			return false;
		}
	}

	@Override
	public boolean handleSensorMessage(ResourceNameEnum resourceName, SensorData data)
	{
		// _Logger.warning("................handleSensorMessage Method Called..................");
		if(data != null){
			_Logger.fine("Handling sensor message: " + data.getName());

			if(data.hasError()){
				_Logger.warning("Error flag set for SensorData instance.");
			}

			String jsonData = DataUtil.getInstance().sensorDataToJson(data);

			_Logger.fine("JSON [SensorData] ->" + jsonData);

			int qos = ConfigConst.DEFAULT_QOS;

			if(this.enablePersistenceClient && this.persistenceClient != null){
				this.persistenceClient.storeData(resourceName.getResourceName(), qos, data);
			}

			this.handleIncomingDataAnalysis(resourceName, data);

			this.handleUpstreamTransmission(resourceName, jsonData, qos);

			return true;
		}else{
			return false;
		}
	}


	@Override
	public boolean handleSystemPerformanceMessage(ResourceNameEnum resourceName, SystemPerformanceData data)
	{
		if(data != null){
			_Logger.info("Handling system performance message: " + data.getName());

			if(data.hasError()){
				_Logger.warning("Error flag set for SystemPerformancedata instance.");
			}

			return true;
		}else{
			return false;
		}
	}
	
	
	public void setActuatorDataListener(String name, IActuatorDataListener listener)
	{
		if(listener != null){
			this.actuatorDataListener = listener;
		}
	}
	
	public void startManager()
	{
		// System Performance Manager
		if(this.sysPerfMgr != null){
			this.sysPerfMgr.startManager();
		}

		//MQTT
		if(this.mqttClient != null){
			if(this.mqttClient.connectClient()){
				_Logger.info("Successfully connected to MQTT client to broker.");

				//Subscriptions

				// Config file data
				// int qos = ConfigConst.DEFAULT_QOS;

				// Action

				// this.mqttClient.subscribeToTopic(ResourceNameEnum.GDA_MGMT_STATUS_CMD_RESOURCE, qos);
				// this.mqttClient.subscribeToTopic(ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE, qos);
				// this.mqttClient.subscribeToTopic(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, qos);
				// this.mqttClient.subscribeToTopic(ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE, qos);
			}else{
				_Logger.severe("Failed to connect MQTT client to broker.");
			}
		}

		// CoAP
		if(this.enableCoapServer && this.coapServer != null){
			// System.out.println("START CoAP SERVER");
			if(this.coapServer.startServer()){
				_Logger.info("CoAP server started.");
			}else{
				_Logger.severe("Failed to start CoAP server. check log file for details");
			}
		}

	}
	
	public void stopManager()
	{
		if(this.sysPerfMgr !=  null){
			this.sysPerfMgr.stopManager();
		}

		if(this.mqttClient != null){
			// UNsubscribes
			this.mqttClient.unsubscribeFromTopic(ResourceNameEnum.GDA_MGMT_STATUS_MSG_RESOURCE);
			this.mqttClient.unsubscribeFromTopic(ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE);
			this.mqttClient.unsubscribeFromTopic(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE);
			this.mqttClient.unsubscribeFromTopic(ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE);

			if (this.mqttClient.disconnectClient()){
				_Logger.info("Successfully disconnected MQTT client from broker.");
			}else{
				_Logger.severe("Failed to disconnect MQTT client from broker.");
			}
		}

		// CoAP
		if(this.enableCoapServer && this.coapServer != null){
		
			if(this.coapServer.stopServer()){
				_Logger.info("CoAP server stopped.");
			}else{
				_Logger.severe("Failed to stop CoAP server. Check log file for details.");
			}
		}
	}

	
	// private methods

	private void handleIncomingDataAnalysis(ResourceNameEnum resourceName, SensorData data){
		
		// check if resource or Sensordata for type
		if(data.getTypeID() == ConfigConst.HUMIDITY_SENSOR_TYPE){
			handleHumiditySensorAnalysis(resourceName, data);
			
		}
		
	}

	private void handleHumiditySensorAnalysis(ResourceNameEnum resource, SensorData data){
		// currently incomplete
		_Logger.info("Analyzing incoming actuator data: " + data.getName());

		boolean isLow = data.getValue() < this.triggerHumidifierFloor;
		boolean isHigh = data.getValue() > this.triggerHumidifierCeiling;

		// ActuatorData ads = new ActuatorData();
		// ads.setName(ConfigConst.HUMIDIFIER_ACTUATOR_NAME);
		// ads.setLocationID(data.getLocationID());
		// ads.setTypeID(ConfigConst.HUMIDIFIER_ACTUATOR_TYPE);
		// ads.setValue(this.nominalHumiditySetting);
		// sendActuatorCommandtoCda(ResourceNameEnum.CDA_ACTUATOR_CMD_RESOURCE, ads);


		if(isLow || isHigh){
				_Logger.info("Humidifier data from CDA exceeded nominal range.");
	
				if(this.latestHumiditySensorData == null){
					// set properties and exit
					this.latestHumiditySensorData = data;
					this.latestHumiditySensorTimeStamp = getDateTimeFromData(data);

					_Logger.info(
						"Starting humidity nominal exception timer. Waiting for seconds: " +
						this.humidityMaxTimePastThreshold
					);

					//return;
	
				} else {
					// _Logger.info(".....................I am Here...................");
					OffsetDateTime curHumiditySensorTimeStamp = getDateTimeFromData(data);

					long diffSeconds = 
						ChronoUnit.SECONDS.between(
							this.latestHumiditySensorTimeStamp, curHumiditySensorTimeStamp);

					_Logger.fine("Checking Humidity value exception time delta: " + diffSeconds);

					if(diffSeconds >= this.humidityMaxTimePastThreshold){
						ActuatorData ad = new ActuatorData();
						ad.setName(ConfigConst.HUMIDIFIER_ACTUATOR_NAME);
						ad.setLocationID(data.getLocationID());
						ad.setTypeID(ConfigConst.HUMIDIFIER_ACTUATOR_TYPE);
						ad.setValue(this.nominalHumiditySetting);

						_Logger.info("******* Hudifier Actuation Triggered ***********");

						if(isLow){
							ad.setCommand(ConfigConst.ON_COMMAND);
						}else{
							ad.setCommand(ConfigConst.OFF_COMMAND);
						}

						_Logger.info("Humidity exceptional value reached. Sending activation event to CDA:" + ad);

						this.lastKnownHumidifierCommand = ad.getCommand();
						sendActuatorCommandtoCda(ResourceNameEnum.CDA_ACTUATOR_CMD_RESOURCE, ad);

						// set ActuatorData and reset SensorData (and timestamp)
						this.latestHumidifierActuatorData = ad;
						this.latestHumiditySensorData = null;
						this.latestHumiditySensorTimeStamp = null;
					}
				}
			} else if(this.lastKnownHumidifierCommand == ConfigConst.ON_COMMAND){
				// check if we need to turn off humidifier
				if(this.latestHumidifierActuatorData != null){
					// check the value - if the humidifier is on, but not yet at nominal, keep it on
					if(this.latestHumidifierActuatorData.getValue() >= this.nominalHumiditySetting){
						this.latestHumidifierActuatorData.setCommand(ConfigConst.OFF_COMMAND);

						_Logger.info("Humidity nominal value reached. Sending OFF actuation event to CDA: " +
						this.latestHumidifierActuatorData);

						sendActuatorCommandtoCda(
							ResourceNameEnum.CDA_ACTUATOR_CMD_RESOURCE, this.latestHumidifierActuatorData);

						// resent ActuatorData and SensorData (and timestamp)
						this.lastKnownHumidifierCommand = this.latestHumidifierActuatorData.getCommand();
						this.latestHumidifierActuatorData = null;
						this.latestHumiditySensorData = null;
						this.latestHumiditySensorTimeStamp = null;
					}else{
						_Logger.fine("Humidifier is still on. Not yet at nominal levels(OK).");
					}
				}else{
					// should'nt happen unless other logic
					// nullifies class scope ActuatorData instance
					_Logger.warning(
						"ERROR: ActuatorData for humidifier is null (Shouldn't be). Can't send command."
					);
				}
			}
	}

	private void handleUpstreamTransmission(ResourceNameEnum resourceName, String jsonData, int qos){
		_Logger.info("TODO: Send JSON data to cloud service: " + resourceName);

		// return false;
	}	
	/**
	 * Initializes the enabled connections. This will NOT start them, but only create the
	 * instances that will be used in the {@link #startManager() and #stopManager()) methods.
	 * 
	 */
	private void initConnections()
	{
		ConfigUtil configUtil = ConfigUtil.getInstance();

		this.enableSystemPerf = 
			configUtil.getBoolean(ConfigConst.GATEWAY_DEVICE, ConfigConst.ENABLE_SYSTEM_PERF_KEY);

		if(this.enableSystemPerf){
			this.sysPerfMgr = new SystemPerformanceManager();
			this.sysPerfMgr.setDataMessageListener(this);
		}

		if(this.enableMqttClient){
			// mqtt client instance
			this.mqttClient = new MqttClientConnector();

			this.mqttClient.setDataMessageListener(this);

		}

		if(this.enableCoapServer){
			_Logger.info("Test Enter here");
			this.coapServer = new CoapServerGateway(this);
		}

		if(this.enableCloudClient){
			// TODO
		}

		if (this.enablePersistenceClient){
			// OPTIONAL TODO
		}
	}

	private void sendActuatorCommandtoCda(ResourceNameEnum resource, ActuatorData data){

		if (this.actuatorDataListener != null){
			this.actuatorDataListener.onActuatorDataUpdate(data);
		}

		// when using mqtt to communicate between the GDA and CDA
		if(this.enableMqttClient && this.mqttClient != null){
			String jsonData = DataUtil.getInstance().actuatorDataToJson(data);

			if(this.mqttClient.publishMessage(resource, jsonData, ConfigConst.DEFAULT_QOS)){
				_Logger.info("Published ActuatorData command from GDA to CDA: "+ data.getCommand());
			}else{
				_Logger.warning(
					"Failed to publish ActuatorData command from GDA to CDA: " + data.getCommand());
			}
		}

	}

	private OffsetDateTime getDateTimeFromData(BaseIotData data){
		OffsetDateTime  odt = null;

		try {
			odt = OffsetDateTime.parse(data.getTimeStamp());
		} catch (Exception e) {
			// TODO: handle exception
			_Logger.warning(
				"Failed to extract ISO 8601 timestamp from IoT data. Using local current time."
			);

			odt = OffsetDateTime.now();

		}
		return odt;
	}
	
}

