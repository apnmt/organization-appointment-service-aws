package de.apnmt.organizationappointment.messaging;

import java.io.IOException;
import java.util.List;

import de.apnmt.aws.common.test.AbstractEventConsumerIT;
import de.apnmt.common.ApnmtTestUtil;
import de.apnmt.common.TopicConstants;
import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.ApnmtEventType;
import de.apnmt.common.event.value.WorkingHourEventDTO;
import de.apnmt.organizationappointment.common.domain.WorkingHour;
import de.apnmt.organizationappointment.common.repository.WorkingHourRepository;
import de.apnmt.organizationappointment.common.service.mapper.WorkingHourEventMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class WorkingHourSqsEventConsumerIT extends AbstractEventConsumerIT {

    @Autowired
    private WorkingHourRepository workingHourRepository;

    @Autowired
    private WorkingHourEventMapper workingHourEventMapper;

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        AbstractEventConsumerIT.beforeAll(TopicConstants.WORKING_HOUR_CHANGED_TOPIC, QueueConstants.WORKING_HOUR_QUEUE);
    }

    @BeforeEach
    public void initTest() {
        this.workingHourRepository.deleteAll();
    }

    @Test
    public void workingHourCreatedTest() throws InterruptedException {
        int databaseSizeBeforeCreate = this.workingHourRepository.findAll().size();
        ApnmtEvent<WorkingHourEventDTO> event = ApnmtTestUtil.createWorkingHourEvent(ApnmtEventType.workingHourCreated);

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.WORKING_HOUR_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<WorkingHour> workingHours = this.workingHourRepository.findAll();
        assertThat(workingHours).hasSize(databaseSizeBeforeCreate + 1);
        WorkingHour workingHour = workingHours.get(workingHours.size() - 1);
        WorkingHourEventDTO workingHourEventDTO = event.getValue();
        assertThat(workingHour.getId()).isEqualTo(workingHourEventDTO.getId());
        assertThat(workingHour.getStartAt()).isEqualTo(workingHourEventDTO.getStartAt());
        assertThat(workingHour.getEndAt()).isEqualTo(workingHourEventDTO.getEndAt());
        assertThat(workingHour.getEmployeeId()).isEqualTo(workingHourEventDTO.getEmployeeId());
    }

    @Test
    public void workingHourUpdatedTest() throws InterruptedException {
        WorkingHourEventDTO eventDTO = ApnmtTestUtil.createWorkingHourEventDTO();
        WorkingHour wh = this.workingHourEventMapper.toEntity(eventDTO);
        this.workingHourRepository.save(wh);

        int databaseSizeBeforeCreate = this.workingHourRepository.findAll().size();
        ApnmtEvent<WorkingHourEventDTO> event = ApnmtTestUtil.createWorkingHourEvent(ApnmtEventType.workingHourCreated);
        event.getValue().setEmployeeId(10L);

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.WORKING_HOUR_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<WorkingHour> workingHours = this.workingHourRepository.findAll();
        assertThat(workingHours).hasSize(databaseSizeBeforeCreate);
        WorkingHour workingHour = workingHours.get(workingHours.size() - 1);
        WorkingHourEventDTO workingHourEventDTO = event.getValue();
        assertThat(workingHour.getId()).isEqualTo(workingHourEventDTO.getId());
        assertThat(workingHour.getStartAt()).isEqualTo(workingHourEventDTO.getStartAt());
        assertThat(workingHour.getEndAt()).isEqualTo(workingHourEventDTO.getEndAt());
        assertThat(workingHour.getEmployeeId()).isNotEqualTo(eventDTO.getEmployeeId());
        assertThat(workingHour.getEmployeeId()).isEqualTo(workingHourEventDTO.getEmployeeId());
    }

    @Test
    public void workingHourDeletedTest() throws InterruptedException {
        ApnmtEvent<WorkingHourEventDTO> event = ApnmtTestUtil.createWorkingHourEvent(ApnmtEventType.workingHourDeleted);
        WorkingHour workingHour = this.workingHourEventMapper.toEntity(event.getValue());
        this.workingHourRepository.save(workingHour);

        int databaseSizeBeforeCreate = this.workingHourRepository.findAll().size();

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.WORKING_HOUR_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<WorkingHour> workingHours = this.workingHourRepository.findAll();
        assertThat(workingHours).hasSize(databaseSizeBeforeCreate - 1);
    }
}
