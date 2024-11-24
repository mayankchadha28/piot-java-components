package programmingtheiot.gda.connection.handlers;

import java.util.logging.Logger;

import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.server.resources.CoapExchange;

import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SystemPerformanceData;
import programmingtheiot.gda.connection.CoapClientConnector;

public class UpdateSystemPerformanceResourceHandler extends CoapResource {
    
    private IDataMessageListener dataMsgListener = null;

    private static final Logger _Logger =
		Logger.getLogger(UpdateSystemPerformanceResourceHandler.class.getName());

    public UpdateSystemPerformanceResourceHandler(String resourceName){
        super(resourceName);
    }

    public void setDataMessageListener(IDataMessageListener listener){
        if(listener != null){
            this.dataMsgListener = listener;
        }
    }

    @Override
    public void handlePUT(CoapExchange context){
        ResponseCode code = ResponseCode.NOT_ACCEPTABLE;

        // notify client that data is being processed
        context.accept();

        if(this.dataMsgListener != null){
            try {
                String jsonData = new String(context.getRequestPayload());

                SystemPerformanceData sysPerfData = DataUtil.getInstance().jsonToSystemPerformanceData(jsonData);

                this.dataMsgListener.handleSystemPerformanceMessage(ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE, sysPerfData);

                code = ResponseCode.CHANGED;
            } catch (Exception e) {
                _Logger.warning(
                    "Failed top handle PUT request. Message: "+ e.getMessage());

                code = ResponseCode.BAD_REQUEST;
            }
        }else{
            _Logger.info("No callback listener for request. Ignoring PUT.");

            code = ResponseCode.CONTINUE;
        }

        String msg = "Update system perf data request handled:" + super.getName();

        context.respond(code, msg);
    }

    @Override
    public void handleGET(CoapExchange context){
        ResponseCode code = ResponseCode.CHANGED;
        context.accept();

        _Logger.info("GET Request invoked");

        String msg = "GET system perf data request handled:"+ super.getName();

        context.respond(code, msg);
    }

    @Override
    public void handlePOST(CoapExchange context){
        ResponseCode code = ResponseCode.CHANGED;
        context.accept();

        _Logger.info("POST Request invoked");

        String msg = "POST system perf data request handled:"+ super.getName();

        context.respond(code, msg);
    }

    @Override
    public void handleDELETE(CoapExchange context){
        ResponseCode code = ResponseCode.CHANGED;
        context.accept();

        _Logger.info("DELETE Request invoked");

        String msg = "DELETE system perf data request handled:"+ super.getName();

        context.respond(code, msg);
    }

}
