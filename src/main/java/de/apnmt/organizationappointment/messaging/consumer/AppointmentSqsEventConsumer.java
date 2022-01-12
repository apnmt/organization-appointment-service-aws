package de.apnmt.organizationappointment.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.value.AppointmentEventDTO;
import de.apnmt.organizationappointment.common.async.controller.AppointmentEventConsumer;
import de.apnmt.organizationappointment.common.service.AppointmentService;
import de.apnmt.organizationappointment.messaging.QueueConstants;
import io.awspring.cloud.messaging.config.annotation.NotificationMessage;
import io.awspring.cloud.messaging.listener.SqsMessageDeletionPolicy;
import io.awspring.cloud.messaging.listener.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class AppointmentSqsEventConsumer extends AppointmentEventConsumer {

    private static final TypeReference<ApnmtEvent<AppointmentEventDTO>> EVENT_TYPE = new TypeReference<>() {
    };

    private final Logger log = LoggerFactory.getLogger(AppointmentSqsEventConsumer.class);

    private final ObjectMapper objectMapper;

    public AppointmentSqsEventConsumer(AppointmentService appointmentService, ObjectMapper objectMapper) {
        super(appointmentService);
        this.objectMapper = objectMapper;
    }

    @SqsListener(value = QueueConstants.APPOINTMENT_QUEUE, deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
    public void receiveEvent(@NotificationMessage String message) {
        try {
            log.info("Received event {} from queue {}", message, QueueConstants.APPOINTMENT_QUEUE);
            ApnmtEvent<AppointmentEventDTO> event = this.objectMapper.readValue(message, EVENT_TYPE);
            super.receiveEvent(event);
        } catch (JsonProcessingException e) {
            log.error("Malformed message {} for queue {}. Event will be ignored.", message, QueueConstants.APPOINTMENT_QUEUE);
        }
    }

}
