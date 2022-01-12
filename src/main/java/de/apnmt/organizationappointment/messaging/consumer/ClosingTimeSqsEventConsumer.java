package de.apnmt.organizationappointment.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.value.ClosingTimeEventDTO;
import de.apnmt.organizationappointment.common.async.controller.ClosingTimeEventConsumer;
import de.apnmt.organizationappointment.common.service.ClosingTimeService;
import de.apnmt.organizationappointment.messaging.QueueConstants;
import io.awspring.cloud.messaging.config.annotation.NotificationMessage;
import io.awspring.cloud.messaging.listener.SqsMessageDeletionPolicy;
import io.awspring.cloud.messaging.listener.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ClosingTimeSqsEventConsumer extends ClosingTimeEventConsumer {

    private static final TypeReference<ApnmtEvent<ClosingTimeEventDTO>> EVENT_TYPE = new TypeReference<>() {
    };

    private final Logger log = LoggerFactory.getLogger(ClosingTimeSqsEventConsumer.class);

    private final ObjectMapper objectMapper;

    public ClosingTimeSqsEventConsumer(ClosingTimeService closingTimeService, ObjectMapper objectMapper) {
        super(closingTimeService);
        this.objectMapper = objectMapper;
    }

    @SqsListener(value = QueueConstants.CLOSING_TIME_QUEUE, deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
    public void receiveEvent(@NotificationMessage String message) {
        try {
            log.info("Received event {} from queue {}", message, QueueConstants.CLOSING_TIME_QUEUE);
            ApnmtEvent<ClosingTimeEventDTO> event = this.objectMapper.readValue(message, EVENT_TYPE);
            super.receiveEvent(event);
        } catch (JsonProcessingException e) {
            log.error("Malformed message {} for queue {}. Event will be ignored.", message, QueueConstants.CLOSING_TIME_QUEUE);
        }
    }

}
