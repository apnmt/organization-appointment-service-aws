package de.apnmt.organizationappointment.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.value.WorkingHourEventDTO;
import de.apnmt.organizationappointment.common.async.controller.WorkingHourEventConsumer;
import de.apnmt.organizationappointment.common.service.WorkingHourService;
import de.apnmt.organizationappointment.messaging.QueueConstants;
import io.awspring.cloud.messaging.config.annotation.NotificationMessage;
import io.awspring.cloud.messaging.listener.SqsMessageDeletionPolicy;
import io.awspring.cloud.messaging.listener.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class WorkingHourSqsEventConsumer extends WorkingHourEventConsumer {

    private static final TypeReference<ApnmtEvent<WorkingHourEventDTO>> EVENT_TYPE = new TypeReference<>() {
    };

    private final Logger log = LoggerFactory.getLogger(WorkingHourSqsEventConsumer.class);

    private final ObjectMapper objectMapper;

    public WorkingHourSqsEventConsumer(WorkingHourService workingHourService, ObjectMapper objectMapper) {
        super(workingHourService);
        this.objectMapper = objectMapper;
    }

    @SqsListener(value = QueueConstants.WORKING_HOUR_QUEUE, deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
    public void receiveEvent(@NotificationMessage String message) {
        try {
            log.info("Received event {} from queue {}", message, QueueConstants.WORKING_HOUR_QUEUE);
            ApnmtEvent<WorkingHourEventDTO> event = this.objectMapper.readValue(message, EVENT_TYPE);
            super.receiveEvent(event);
        } catch (JsonProcessingException e) {
            log.error("Malformed message {} for queue {}. Event will be ignored.", message, QueueConstants.WORKING_HOUR_QUEUE);
        }
    }

}
