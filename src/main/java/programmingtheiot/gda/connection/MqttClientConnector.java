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
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;

/**
 * MQTT Connector that handles calls to publish, subscribe / unsubscribe and disconnect.
 *  It also sets the mode to async or sync
 * 
 */
public class MqttClientConnector implements IPubSubClient, MqttCallbackExtended
{
	// Private Vars
	private boolean useAsyncClient = false;

	private MqttClient mqttClient = null;
	private MqttConnectOptions connOpts = null;
	private MemoryPersistence persistence = null;
	private IDataMessageListener dataMsgListener = null;

	private String clientID = null;
	private String brokerAddr = null;
	private String host = ConfigConst.DEFAULT_HOST;
	private String protocol = ConfigConst.DEFAULT_MQTT_PROTOCOL;
	private int port = ConfigConst.DEFAULT_MQTT_PORT;
	private int brokerKeepAlive = ConfigConst.DEFAULT_KEEP_ALIVE;

	
	// static
	
	private static final Logger _Logger =
		Logger.getLogger(MqttClientConnector.class.getName());
	
	// params
	
	
	// constructors
	
	/**
	 * Default.
	 * 
	 */
	public MqttClientConnector()
	{
		super();

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
	}
	
	
	// public methods
	
	@Override
	public boolean connectClient()
	{
		try {
			if(this.mqttClient == null){
				this.mqttClient = new MqttClient(this.brokerAddr, this.clientID, this.persistence);
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

		if(qos < 0 || qos > 2){
			qos = ConfigConst.DEFAULT_QOS;
		}

		try {
			byte[] payload = msg.getBytes();
			MqttMessage mqttMsg = new MqttMessage(payload);
			mqttMsg.setQos(qos);
			this.mqttClient.publish(topicName.getResourceName(), mqttMsg);
			return true;
		} catch (Exception e) {
			// TODO: handle exception
			_Logger.log(Level.SEVERE, "Failed to publish message to topic: "+ topicName, e);
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

		if(qos < 0 || qos >2){
			qos = ConfigConst.DEFAULT_QOS;
		}

		try {
			this.mqttClient.subscribe(topicName.getResourceName(), qos);
			_Logger.info("Successfully subscribed to topic: " + topicName.getResourceName());
			return true;
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to subscribe to topic: " + topicName, e);
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

		try {
			this.mqttClient.unsubscribe(topicName.getResourceName());
			_Logger.info("Successfully unsubscriibed from topic: " + topicName.getResourceName());
			return true;
		} catch (Exception e) {
			// TODO: handle exception
			_Logger.log(Level.SEVERE, "Failed to unsubscribe from topic: " + topicName, e);

		}
		return false;
	}

	@Override
	public boolean setConnectionListener(IConnectionListener listener)
	{
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
		// TODO: implement this
	}
	
	/**
	 * Called by {@link #initClientParameters(String)} to load credentials.
	 * 
	 * @param configSectionName The name of the configuration section to use for
	 * the MQTT client configuration parameters.
	 */
	private void initCredentialConnectionParameters(String configSectionName)
	{
		// TODO: implement this
	}
	
	/**
	 * Called by {@link #initClientParameters(String)} to enable encryption.
	 * 
	 * @param configSectionName The name of the configuration section to use for
	 * the MQTT client configuration parameters.
	 */
	private void initSecureConnectionParameters(String configSectionName)
	{
		// TODO: implement this
	}
}
