/**
 * This class is part of the Programming the Internet of Things project.
 * 
 * It is provided as a simple shell to guide the student and assist with
 * implementation for the Programming the Internet of Things exercises,
 * and designed to be modified by the student as needed.
 */ 

package programmingtheiot.gda.connection;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import programmingtheiot.common.ResourceNameEnum;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;

import java.io.File;
import java.io.ObjectInputFilter.Config;

import javax.net.ssl.SSLSocketFactory;

import programmingtheiot.common.SimpleCertManagementUtil;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SensorData;



/**
 * MQTT Connector that handles calls to publish, subscribe / unsubscribe and disconnect.
 *  It also sets the mode to async or sync
 * 
 */
public class MqttClientConnector implements IPubSubClient, MqttCallbackExtended
{
	// Private Vars
	private boolean useAsyncClient = false;

	// private MqttClient mqttClient = null;
	private MqttAsyncClient mqttClient = null;
	private MqttConnectOptions connOpts = null;
	private MemoryPersistence persistence = null;
	private IDataMessageListener dataMsgListener = null;

	private String clientID = null;
	private String brokerAddr = null;
	private String host = ConfigConst.DEFAULT_HOST;
	private String protocol = ConfigConst.DEFAULT_MQTT_PROTOCOL;
	private int port = ConfigConst.DEFAULT_MQTT_PORT;
	private int brokerKeepAlive = ConfigConst.DEFAULT_KEEP_ALIVE;

	private String pemFileName = null;
	private boolean enableEncryption = false;
	private boolean useCleanSession = false;
	private boolean enableAutoReconnect = true;

	private IConnectionListener connListener = null;
	private boolean useCloudGatewayConfig = false;
	
	// static
	
	private static final Logger _Logger =
		Logger.getLogger(MqttClientConnector.class.getName());
	
	// params
	
	
	// constructors
	
	/**
	 * Default.
	 * 
	 */

	public MqttClientConnector(){
		this(false);
	} 

	public MqttClientConnector(boolean useCloudGatewayConfig){
		this(useCloudGatewayConfig ? ConfigConst.CLOUD_GATEWAY_SERVICE : null);
	}

	public MqttClientConnector(String cloudGatewayConfigSectionName)
	{
		super();

		// initClientParameters(ConfigConst.MQTT_GATEWAY_SERVICE);


		ConfigUtil configUtil = ConfigUtil.getInstance();

		this.host = configUtil.getProperty(ConfigConst.MQTT_GATEWAY_SERVICE, 
			ConfigConst.HOST_KEY, ConfigConst.DEFAULT_HOST);

		this.port = configUtil.getInteger(
			ConfigConst.MQTT_GATEWAY_SERVICE, ConfigConst.PORT_KEY, ConfigConst.DEFAULT_MQTT_PORT
		);

		this.brokerKeepAlive = 
			configUtil.getInteger(
				ConfigConst.MQTT_GATEWAY_SERVICE, ConfigConst.KEEP_ALIVE_KEY, ConfigConst.DEFAULT_KEEP_ALIVE
			);

		this.useAsyncClient = 
			configUtil.getBoolean(
				ConfigConst.MQTT_GATEWAY_SERVICE, ConfigConst.USE_ASYNC_CLIENT_KEY
			);

		//MQTT client ID required by paho 
		this.clientID = MqttClient.generateClientId();

		// Specific to MQTT connection
		this.persistence = new MemoryPersistence();
		this.connOpts = new MqttConnectOptions();

		this.connOpts.setKeepAliveInterval(this.brokerKeepAlive);

		// if random clientID, clean sesson will be 'true'
		this.connOpts.setCleanSession(false);

		// Auto reconnect for connection recovery
		this.connOpts.setAutomaticReconnect(true);

		// url construct
		this.brokerAddr = this.protocol + "://" + this.host + ":" + this.port;

		//set cloud related parameters if available
		if(cloudGatewayConfigSectionName != null && cloudGatewayConfigSectionName.trim().length() > 0){
			this.useCloudGatewayConfig = true;

			initClientParameters(cloudGatewayConfigSectionName);
		}else{
			this.useCloudGatewayConfig = false;

			initClientParameters(ConfigConst.MQTT_GATEWAY_SERVICE);
		}

	}
	
	
	// public methods
	
	@Override
	public boolean connectClient()
	{
		try {
			if(this.mqttClient == null){
				this.mqttClient = new MqttAsyncClient(this.brokerAddr, this.clientID, this.persistence);
				this.mqttClient.setCallback(this);
			}	

			if(! this.mqttClient.isConnected()){
				_Logger.info("MQTT client connecting to broker:" + this.brokerAddr);
				this.mqttClient.connect(this.connOpts);
				return true;

			}else{
				_Logger.warning("MQTT client already connected to broker:" + this.brokerAddr);
			}

		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to connect Mqtt client to broker.", e);
		}

		return false;
	}

	@Override
	public boolean disconnectClient()
	{
		try {
			if(this.mqttClient != null){
				if(this.mqttClient.isConnected()){
					_Logger.info("Disconncting MQTT client from broker: "+ this.brokerAddr);
					this.mqttClient.disconnect();
					return true;
				}else{
					_Logger.warning("MQTT client not connected to broker: "+ this.brokerAddr);
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			_Logger.log(Level.SEVERE, "Failed to disconnect MQTT client from broker: "
			+ this.brokerAddr, e);
		}

		return false;
	}

	public boolean isConnected()
	{
		return (this.mqttClient != null && this.mqttClient.isConnected());
	}

	protected boolean publishMessage(String topicName, byte[] payload, int qos){
		if (topicName == null){
			_Logger.warning("Resource is null. Unable to publish message: "+ this.brokerAddr);
			return false;
		}

		if (payload == null || payload.length == 0){
			_Logger.warning("Message is null or empty. Unable to publish message: " + this.brokerAddr);
			return false;
		}

		if(qos < 0 || qos > 2){
			_Logger.warning("Invalid QoS. Using default. QoS requested: " + qos);

			qos = ConfigConst.DEFAULT_QOS;
		}

		try {
			
			MqttMessage mqttMsg = new MqttMessage();
			mqttMsg.setQos(qos);
			mqttMsg.setPayload(payload);

			this.mqttClient.publish(topicName, mqttMsg);

			return true;
		} catch (Exception e) {
			// TODO: handle exception
			_Logger.log(Level.SEVERE, "Failed to publish message to topic: "+ topicName, e);
		}

		return false;
	}
	
	@Override
	public boolean publishMessage(ResourceNameEnum topicName, String msg, int qos)
	{
		// TODO
		if (topicName == null){
			_Logger.warning("Resource is null. Unable to publish message: "+ this.brokerAddr);
			return false;
		}

		if(msg == null || msg.length() == 0){
			_Logger.warning("Message is null or empty. Unable to publish message: " + this.brokerAddr);
			return false;
		}

		return publishMessage(topicName.getResourceName(), msg.getBytes(), qos);
	}

	protected boolean subscribeToTopic(String topicName, int qos){
		return subscribeToTopic(topicName, qos);
	}

	protected boolean subscribeToTopic(String topicName, int qos, IMqttMessageListener listener){
		if(topicName == null){
			_Logger.warning("Resource is null. Unable to subscribe to topic: " + this.brokerAddr);

			return false;
		}

		if(qos < 0 || qos > 2){
			_Logger.warning("Invalid QoS. Using default. Qos requested: " + qos);

			qos = ConfigConst.DEFAULT_QOS;
		}

		try {
			if(listener != null){
				this.mqttClient.subscribe(topicName, qos, listener);

				_Logger.info("Successfully subscribed to topic with listener: " + topicName);
			}else{
				this.mqttClient.subscribe(topicName, qos);

				_Logger.info("Successfully subscribed to topic: " + topicName);
			}
			
			return true;

		} catch (Exception e) {
			// TODO: handle exception
			_Logger.log(Level.SEVERE, "Failed to subscribe to topic: " + topicName, e);
		}

		return false;
	}

	@Override
	public boolean subscribeToTopic(ResourceNameEnum topicName, int qos)
	{
		if (topicName == null){
			_Logger.warning("Resource is null. Unable to publish message: "+ this.brokerAddr);
			return false;
		}

		return subscribeToTopic(topicName.getResourceName(), qos);
	}

	protected boolean unsubscribeFromTopic(String topicName){
		if(topicName == null){
			_Logger.warning("Resource is null. Unabble to unsubscribe from topic: " + this.brokerAddr);

			return false;
		}

		try {
			this.mqttClient.unsubscribe(topicName);

			_Logger.info("Successfully unsubscribed from topic: " + topicName);

			return true;
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to unsubscribe from topic: " + topicName, e);
		}

		return false;
	}

	@Override
	public boolean unsubscribeFromTopic(ResourceNameEnum topicName)
	{
		if (topicName == null){
			_Logger.warning("Resource is null. Unable to publish message: "+ this.brokerAddr);
			return false;
		}

		return unsubscribeFromTopic(topicName.getResourceName());
	}

	@Override
	public boolean setConnectionListener(IConnectionListener listener)
	{
		// set connection listener to notify external listeners of connect and disconnect events.
		
		if(listener != null){
			_Logger.info("Setting connection listener.");
			this.connListener = listener;
			return true;
		}else{
			_Logger.warning("No connection listener specified. Ignoring.");
		}

		return false;
	}
	
	@Override
	public boolean setDataMessageListener(IDataMessageListener listener)
	{
		if(listener != null){
			this.dataMsgListener = listener;
			return true;
		}

		return false;
	}
	
	// callbacks
	
	@Override
	public void connectComplete(boolean reconnect, String serverURI)
	{
		_Logger.info("MQTT connection successful (is reconnect = " + reconnect + "). Broker: " + serverURI);

		int qos= 1;
		
		this.subscribeToTopic(ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE, qos);
		this.subscribeToTopic(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, qos);
		this.subscribeToTopic(ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE, qos);



	}

	@Override
	public void connectionLost(Throwable t)
	{
		_Logger.log(Level.WARNING, "Lost connection to MQTT broker: "+ this.brokerAddr, t);
	}
	
	@Override
	public void deliveryComplete(IMqttDeliveryToken token)
	{
		_Logger.fine("Delivered MQTT message with ID: "+ token.getMessageId());
	}
	
	@Override
	public void messageArrived(String topic, MqttMessage msg) throws Exception
	{
		_Logger.info("MQTT message arrived on topic: "+ topic + "'");

		SensorData sensorData = DataUtil.getInstance().jsonToSensorData(new String(msg.getPayload()));
		
		if(this.dataMsgListener != null){
			_Logger.info("................Handling Sensor Data.................");
			this.dataMsgListener.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sensorData);
		}

	}

	
	// private methods
	
	/**
	 * Called by the constructor to set the MQTT client parameters to be used for the connection.
	 * 
	 * @param configSectionName The name of the configuration section to use for
	 * the MQTT client configuration parameters.
	 */
	private void initClientParameters(String configSectionName)
	{
		// This sets the client parameters by loading from the ConfigConst and PiotConfig.props

		ConfigUtil configUtil = ConfigUtil.getInstance();

		this.host = configUtil.getProperty(configSectionName, ConfigConst.HOST_KEY, ConfigConst.DEFAULT_HOST);

		this.port = configUtil.getInteger(configSectionName, ConfigConst.PORT_KEY, ConfigConst.DEFAULT_MQTT_PORT);

		this.brokerKeepAlive = 
			configUtil.getInteger(configSectionName, ConfigConst.KEEP_ALIVE_KEY, ConfigConst.DEFAULT_KEEP_ALIVE);

		this.enableEncryption = 
			configUtil.getBoolean(configSectionName, ConfigConst.ENABLE_CRYPT_KEY);

		this.pemFileName = 
			configUtil.getProperty(configSectionName, ConfigConst.CERT_FILE_KEY);

		// Optional properties
		this.useAsyncClient = 
			configUtil.getBoolean(ConfigConst.MQTT_GATEWAY_SERVICE, ConfigConst.USE_ASYNC_CLIENT_KEY);

		this.clientID = 
			configUtil.getProperty(ConfigConst.GATEWAY_SERVICE, ConfigConst.DEVICE_LOCATION_ID_KEY, MqttClient.generateClientId());

		// options specific to MQTT connection
		this.persistence = new MemoryPersistence();
		this.connOpts = new MqttConnectOptions();

		this.connOpts.setKeepAliveInterval(this.brokerKeepAlive);
		this.connOpts.setCleanSession(this.useCleanSession);
		this.connOpts.setAutomaticReconnect(this.enableAutoReconnect);

		// if encryption enabled, try to load and apply certs
		if(this.enableEncryption){
			initSecureConnectionParameters(configSectionName);
		}

		// if there is credential file, try to load and apply them
		if(configUtil.hasProperty(configSectionName, ConfigConst.CRED_FILE_KEY)){
			initCredentialConnectionParameters(configSectionName);
		}

		// constructing the url
		this.brokerAddr = this.protocol + "://" + this.host + ":" + this.port;

		_Logger.info("Using URL for broker conn: " + this.brokerAddr);
	}
	
	/**
	 * Called by {@link #initClientParameters(String)} to load credentials.
	 * 
	 * @param configSectionName The name of the configuration section to use for
	 * the MQTT client configuration parameters.
	 */
	private void initCredentialConnectionParameters(String configSectionName)
	{
		// This method checks for the credential file and sets the credentials

		ConfigUtil configUtil = ConfigUtil.getInstance();

		try {
			_Logger.info("Checking if credentials file exists and is loadable...");

			Properties props = configUtil.getCredentials(configSectionName);

			if(props != null){
				this.connOpts.setUserName(props.getProperty(ConfigConst.USER_AUTH_TOKEN_KEY, ""));
				this.connOpts.setPassword(props.getProperty(ConfigConst.USER_AUTH_TOKEN_KEY, "").toCharArray());

				_Logger.info("Credentials now set.");

			}else{
				_Logger.warning("No credentials are set.");
			}

		} catch (Exception e) {
			// TODO: handle exception
			_Logger.log(Level.WARNING, "Credential file non-existent. Disabling auth requirement.");
		}
	}
	
	/**
	 * Called by {@link #initClientParameters(String)} to enable encryption.
	 * 
	 * @param configSectionName The name of the configuration section to use for
	 * the MQTT client configuration parameters.
	 */
	private void initSecureConnectionParameters(String configSectionName)
	{
		// This class enables the TLS by reading the pem file from the this.pemFileName

		ConfigUtil configUtil = ConfigUtil.getInstance();

		try {
			_Logger.info("Configuring TLS...");

			if(this.pemFileName != null){
				File file = new File(this.pemFileName);

				if(file.exists()){
					_Logger.info("PEM file valid. Using secure connection: " + this.pemFileName);
				}else{
					this.enableEncryption = false;

					_Logger.log(Level.WARNING, "PEM file invalid. Using insecure connection: " + this.pemFileName, new Exception());

					return;
				}

				SSLSocketFactory sslFactory = 
					SimpleCertManagementUtil.getInstance().loadCertificate(this.pemFileName);

				this.connOpts.setSocketFactory(sslFactory);

				//override current config params
				this.port = 
					configUtil.getInteger(configSectionName, ConfigConst.SECURE_PORT_KEY, ConfigConst.DEFAULT_MQTT_SECURE_PORT);

				this.protocol = ConfigConst.DEFAULT_MQTT_SECURE_PROTOCOL;

				_Logger.info("TLS enabled.");

			}
		} catch (Exception e) {
			// TODO: handle exception
			_Logger.log(Level.SEVERE, "Failed to initialize secure MQTT connection. Using insecure connection.", e);

			this.enableEncryption = false;
		}

	}
}
