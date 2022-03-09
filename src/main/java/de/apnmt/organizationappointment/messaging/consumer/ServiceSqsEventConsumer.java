package de.apnmt.organizationappointment.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.apnmt.aws.common.config.AwsCloudProperties;
import de.apnmt.aws.common.util.TracingUtil;
import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.value.ServiceEventDTO;
import de.apnmt.organizationappointment.common.async.controller.ServiceEventConsumer;
import de.apnmt.organizationappointment.common.service.ServiceService;
import de.apnmt.organizationappointment.messaging.QueueConstants;
import io.awspring.cloud.messaging.config.annotation.NotificationMessage;
import io.awspring.cloud.messaging.listener.SqsMessageDeletionPolicy;
import io.awspring.cloud.messaging.listener.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

@Controller
public class ServiceSqsEventConsumer extends ServiceEventConsumer {

    private static final TypeReference<ApnmtEvent<ServiceEventDTO>> EVENT_TYPE = new TypeReference<>() {
    };

    private final Logger log = LoggerFactory.getLogger(ServiceSqsEventConsumer.class);

    @Value("${spring.application.name}")
    public String appName;

    private final ObjectMapper objectMapper;
    private final AwsCloudProperties awsCloudProperties;

    public ServiceSqsEventConsumer(ServiceService serviceService, ObjectMapper objectMapper, AwsCloudProperties awsCloudProperties) {
        super(serviceService);
        this.objectMapper = objectMapper;
        this.awsCloudProperties = awsCloudProperties;
    }

    @SqsListener(value = QueueConstants.SERVICE_QUEUE, deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
    public void receiveEvent(@NotificationMessage String message) {
        try {
            log.info("Received event {} from queue {}", message, QueueConstants.SERVICE_QUEUE);
            ApnmtEvent<ServiceEventDTO> event = this.objectMapper.readValue(message, EVENT_TYPE);
            log.info("Received TraceId: {}", event.getTraceId());
            TracingUtil.beginTracing(appName, event.getTraceId(), awsCloudProperties.getTracing().getXRay().isEnabled());
            super.receiveEvent(event);
        } catch (JsonProcessingException e) {
            log.error("Malformed message {} for queue {}. Event will be ignored.", message, QueueConstants.SERVICE_QUEUE);
            TracingUtil.addException(e, awsCloudProperties.getTracing().getXRay().isEnabled());
        } finally {
            TracingUtil.endTracing(awsCloudProperties.getTracing().getXRay().isEnabled());
        }
    }

}
