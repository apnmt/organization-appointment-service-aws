package de.apnmt.organizationappointment.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.stereotype.Service;

@Service
public class ServiceSqsEventConsumer extends ServiceEventConsumer {

    private static final TypeReference<ApnmtEvent<ServiceEventDTO>> EVENT_TYPE = new TypeReference<>() {
    };

    private final Logger log = LoggerFactory.getLogger(ServiceSqsEventConsumer.class);

    private final ObjectMapper objectMapper;

    public ServiceSqsEventConsumer(ServiceService serviceService, ObjectMapper objectMapper) {
        super(serviceService);
        this.objectMapper = objectMapper;
    }

    @SqsListener(value = QueueConstants.SERVICE_QUEUE, deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
    public void receiveEvent(@NotificationMessage String message) {
        try {
            log.info("Received event {} from queue {}", message, QueueConstants.SERVICE_QUEUE);
            ApnmtEvent<ServiceEventDTO> event = this.objectMapper.readValue(message, EVENT_TYPE);
            super.receiveEvent(event);
        } catch (JsonProcessingException e) {
            log.error("Malformed message {} for queue {}. Event will be ignored.", message, QueueConstants.SERVICE_QUEUE);
        }
    }

}
