/**
 * This class is part of the Programming the Internet of Things project.
 * 
 * It is provided as a simple shell to guide the student and assist with
 * implementation for the Programming the Internet of Things exercises,
 * and designed to be modified by the student as needed.
 */ 

package programmingtheiot.gda.connection;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.CoapServer;
import org.eclipse.californium.core.config.CoapConfig;
import org.eclipse.californium.core.network.Endpoint;
import org.eclipse.californium.core.network.interceptors.MessageTracer;
import org.eclipse.californium.core.server.resources.Resource;
import org.eclipse.californium.elements.config.UdpConfig;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.gda.connection.handlers.GenericCoapResourceHandler;
import programmingtheiot.gda.connection.handlers.GetActuatorCommandResourceHandler;
import programmingtheiot.gda.connection.handlers.UpdateSystemPerformanceResourceHandler;
import programmingtheiot.gda.connection.handlers.UpdateTelemetryResourceHandler;

/**
 * Method to manage and create CoAP server instance
 * 
 */
public class CoapServerGateway
{
	// static
	// static {
	// 	CoapConfig.register();
	// 	UdpConfig.register();
	// }

	private static final Logger _Logger =
		Logger.getLogger(CoapServerGateway.class.getName());
	
	// params
	
	private CoapServer coapServer = null;
	private IDataMessageListener dataMsgListener = null;
	
	
	// constructors
	
	/**
	 * Constructor.
	 * 
	 * @param dataMsgListener
	 */
	public CoapServerGateway(IDataMessageListener dataMsgListener)
	{
		super();
		
		/*
		 * Basic constructor implementation provided. Change as needed.
		 */
		
		this.dataMsgListener = dataMsgListener;
		
		initServer();
	}

		
	// public methods
	
	public void addResource(ResourceNameEnum resourceType, String endName, Resource resource)
	{
		if(resourceType != null && resource != null){
			createAndAddResourceChain(resourceType, resource);
		}
	}
	
	public boolean hasResource(String name)
	{
		return false;
	}
	
	public void setDataMessageListener(IDataMessageListener listener)
	{
		if(listener != null){
			this.dataMsgListener = listener;
		}
	}
	
	public boolean startServer()
	{
		try {
			if(this.coapServer != null){
				this.coapServer.start();

				for(Endpoint ep: this.coapServer.getEndpoints()){
					ep.addInterceptor(new MessageTracer());
				}

				return true;
			}else{
				_Logger.warning("CoAP server START failed. Not yet initialized.");
			}
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to start CoAP server.", e);
		}

		return false;
	}
	
	public boolean stopServer()
	{
		try {
			if(this.coapServer != null){
				this.coapServer.stop();

				return true;
			}
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to stop CoAP server.", e);
		}
		return false;
	}
	
	
	// private methods
	
	private void createAndAddResourceChain(ResourceNameEnum resourceType, Resource resource)
	{
		_Logger.info("Adding server resource handler chain: "+ resourceType.getResourceName());

		List<String> resourceNames = resourceType.getResourceNameChain();
		Queue<String> queue = new ArrayBlockingQueue<>(resourceNames.size());

		queue.addAll(resourceNames);

		// check for parent resource
		Resource parentResource = this.coapServer.getRoot();

		// if no parent resource add it now
		if(parentResource == null){
			parentResource = new CoapResource(queue.poll());
			this.coapServer.add(parentResource);
		}

		while(! queue.isEmpty()){
			// get next resource name
			String resourceName = queue.poll();
			Resource nextResource = parentResource.getChild(resourceName);

			if(nextResource == null){
				if(queue.isEmpty()){
					nextResource = resource;
					nextResource.setName(resourceName);
				}else{
					nextResource = new CoapResource(resourceName);
				}

				parentResource.add(nextResource);
			}

			parentResource = nextResource;
		}
	}
	
	private void initServer(ResourceNameEnum ...resources)
	{
		this.coapServer = new CoapServer();

		initDefaultResources();
	}

	private void initDefaultResources(){
		// initialize predefined resources
		GetActuatorCommandResourceHandler getActuatorCommandResourceHandler = 
			new GetActuatorCommandResourceHandler(
				ResourceNameEnum.CDA_ACTUATOR_CMD_RESOURCE.getResourceType()
			);

		if(this.dataMsgListener != null){
			this.dataMsgListener.setActuatorDataListener(null, 
			getActuatorCommandResourceHandler);
		}

		addResource(ResourceNameEnum.CDA_ACTUATOR_CMD_RESOURCE,
		 null, getActuatorCommandResourceHandler);

		// UpdateTelemetry
		UpdateTelemetryResourceHandler updateTelemetryResourceHandler = 
			new UpdateTelemetryResourceHandler(
				ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE.getResourceType());

		updateTelemetryResourceHandler.setDataMessageListener(this.dataMsgListener);

		addResource(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, null, 
			updateTelemetryResourceHandler);
		
		// Update System Performance
			UpdateSystemPerformanceResourceHandler updateSystemPerformanceResourceHandler = 
			new UpdateSystemPerformanceResourceHandler(
				ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE.getResourceType());

		updateSystemPerformanceResourceHandler.setDataMessageListener(dataMsgListener);

		addResource(ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE, null, 
		    updateSystemPerformanceResourceHandler);	

		


		// 

		

	}
}
