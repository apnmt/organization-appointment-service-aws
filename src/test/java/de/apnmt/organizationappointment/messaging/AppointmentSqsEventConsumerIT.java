package de.apnmt.organizationappointment.messaging;

import java.io.IOException;
import java.util.List;

import de.apnmt.aws.common.test.AbstractEventConsumerIT;
import de.apnmt.common.ApnmtTestUtil;
import de.apnmt.common.TopicConstants;
import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.ApnmtEventType;
import de.apnmt.common.event.value.AppointmentEventDTO;
import de.apnmt.organizationappointment.common.domain.Appointment;
import de.apnmt.organizationappointment.common.repository.AppointmentRepository;
import de.apnmt.organizationappointment.common.service.mapper.AppointmentEventMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class AppointmentSqsEventConsumerIT extends AbstractEventConsumerIT {

    @Autowired
    private AppointmentRepository appointmentRepository;

    @Autowired
    private AppointmentEventMapper appointmentEventMapper;

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        AbstractEventConsumerIT.beforeAll(TopicConstants.APPOINTMENT_CHANGED_TOPIC, QueueConstants.APPOINTMENT_QUEUE);
    }

    @BeforeEach
    public void initTest() {
        this.appointmentRepository.deleteAll();
    }

    @Test
    void appointmentCreatedTest() throws InterruptedException {
        int databaseSizeBeforeCreate = this.appointmentRepository.findAll().size();
        ApnmtEvent<AppointmentEventDTO> event = ApnmtTestUtil.createAppointmentEvent(ApnmtEventType.appointmentCreated);

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.APPOINTMENT_CHANGED_TOPIC, event);

        Thread.sleep(5000);

        List<Appointment> appointments = this.appointmentRepository.findAll();
        assertThat(appointments).hasSize(databaseSizeBeforeCreate + 1);
        Appointment appointment = appointments.get(appointments.size() - 1);
        AppointmentEventDTO appointmentEventDTO = event.getValue();
        assertThat(appointment.getId()).isEqualTo(appointmentEventDTO.getId());
        assertThat(appointment.getStartAt()).isEqualTo(appointmentEventDTO.getStartAt());
        assertThat(appointment.getEndAt()).isEqualTo(appointmentEventDTO.getEndAt());
        assertThat(appointment.getEmployeeId()).isEqualTo(appointmentEventDTO.getEmployeeId());
        assertThat(appointment.getOrganizationId()).isEqualTo(appointmentEventDTO.getOrganizationId());
    }

    @Test
    void appointmentUpdatedTest() throws InterruptedException {
        AppointmentEventDTO eventDTO = ApnmtTestUtil.createAppointmentEventDTO();
        Appointment apnmt = this.appointmentEventMapper.toEntity(eventDTO);
        this.appointmentRepository.save(apnmt);

        int databaseSizeBeforeCreate = this.appointmentRepository.findAll().size();
        ApnmtEvent<AppointmentEventDTO> event = ApnmtTestUtil.createAppointmentEvent(ApnmtEventType.appointmentCreated);
        event.getValue().setEmployeeId(10L);

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.APPOINTMENT_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<Appointment> appointments = this.appointmentRepository.findAll();
        assertThat(appointments).hasSize(databaseSizeBeforeCreate);
        Appointment appointment = appointments.get(appointments.size() - 1);
        AppointmentEventDTO appointmentEventDTO = event.getValue();
        assertThat(appointment.getId()).isEqualTo(appointmentEventDTO.getId());
        assertThat(appointment.getStartAt()).isEqualTo(appointmentEventDTO.getStartAt());
        assertThat(appointment.getEndAt()).isEqualTo(appointmentEventDTO.getEndAt());
        assertThat(appointment.getEmployeeId()).isNotEqualTo(eventDTO.getEmployeeId());
        assertThat(appointment.getEmployeeId()).isEqualTo(appointmentEventDTO.getEmployeeId());
        assertThat(appointment.getOrganizationId()).isEqualTo(appointmentEventDTO.getOrganizationId());
    }

    @Test
    void appointmentDeletedTest() throws InterruptedException {
        ApnmtEvent<AppointmentEventDTO> event = ApnmtTestUtil.createAppointmentEvent(ApnmtEventType.appointmentDeleted);
        Appointment appointment = this.appointmentEventMapper.toEntity(event.getValue());
        this.appointmentRepository.save(appointment);

        int databaseSizeBeforeCreate = this.appointmentRepository.findAll().size();

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.APPOINTMENT_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<Appointment> appointments = this.appointmentRepository.findAll();
        assertThat(appointments).hasSize(databaseSizeBeforeCreate - 1);
    }

}
