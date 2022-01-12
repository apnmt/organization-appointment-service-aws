package de.apnmt.organizationappointment.messaging;

import java.io.IOException;
import java.util.List;

import de.apnmt.aws.common.test.AbstractEventConsumerIT;
import de.apnmt.common.ApnmtTestUtil;
import de.apnmt.common.TopicConstants;
import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.ApnmtEventType;
import de.apnmt.common.event.value.ClosingTimeEventDTO;
import de.apnmt.organizationappointment.common.domain.ClosingTime;
import de.apnmt.organizationappointment.common.repository.ClosingTimeRepository;
import de.apnmt.organizationappointment.common.service.mapper.ClosingTimeEventMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class ClosingTimeSqsEventConsumerIT extends AbstractEventConsumerIT {

    @Autowired
    private ClosingTimeRepository closingTimeRepository;

    @Autowired
    private ClosingTimeEventMapper closingTimeEventMapper;

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        AbstractEventConsumerIT.beforeAll(TopicConstants.CLOSING_TIME_CHANGED_TOPIC, QueueConstants.CLOSING_TIME_QUEUE);
    }

    @BeforeEach
    public void initTest() {
        this.closingTimeRepository.deleteAll();
    }

    @Test
    public void closingTimeCreatedTest() throws InterruptedException {
        int databaseSizeBeforeCreate = this.closingTimeRepository.findAll().size();
        ApnmtEvent<ClosingTimeEventDTO> event = ApnmtTestUtil.createClosingTimeEvent(ApnmtEventType.closingTimeCreated);

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.CLOSING_TIME_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<ClosingTime> closingTimes = this.closingTimeRepository.findAll();
        assertThat(closingTimes).hasSize(databaseSizeBeforeCreate + 1);
        ClosingTime closingTime = closingTimes.get(closingTimes.size() - 1);
        ClosingTimeEventDTO closingTimeEventDTO = event.getValue();
        assertThat(closingTime.getId()).isEqualTo(closingTimeEventDTO.getId());
        assertThat(closingTime.getStartAt()).isEqualTo(closingTimeEventDTO.getStartAt());
        assertThat(closingTime.getEndAt()).isEqualTo(closingTimeEventDTO.getEndAt());
        assertThat(closingTime.getOrganizationId()).isEqualTo(closingTimeEventDTO.getOrganizationId());
    }

    @Test
    public void closingTimeUpdatedTest() throws InterruptedException {
        ClosingTimeEventDTO eventDTO = ApnmtTestUtil.createClosingTimeEventDTO();
        ClosingTime ct = this.closingTimeEventMapper.toEntity(eventDTO);
        this.closingTimeRepository.save(ct);

        int databaseSizeBeforeCreate = this.closingTimeRepository.findAll().size();
        ApnmtEvent<ClosingTimeEventDTO> event = ApnmtTestUtil.createClosingTimeEvent(ApnmtEventType.closingTimeCreated);
        event.getValue().setOrganizationId(10L);

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.CLOSING_TIME_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<ClosingTime> closingTimes = this.closingTimeRepository.findAll();
        assertThat(closingTimes).hasSize(databaseSizeBeforeCreate);
        ClosingTime closingTime = closingTimes.get(closingTimes.size() - 1);
        ClosingTimeEventDTO closingTimeEventDTO = event.getValue();
        assertThat(closingTime.getId()).isEqualTo(closingTimeEventDTO.getId());
        assertThat(closingTime.getStartAt()).isEqualTo(closingTimeEventDTO.getStartAt());
        assertThat(closingTime.getEndAt()).isEqualTo(closingTimeEventDTO.getEndAt());
        assertThat(closingTime.getOrganizationId()).isNotEqualTo(eventDTO.getOrganizationId());
        assertThat(closingTime.getOrganizationId()).isEqualTo(closingTimeEventDTO.getOrganizationId());
    }

    @Test
    public void closingTimeDeletedTest() throws InterruptedException {
        ApnmtEvent<ClosingTimeEventDTO> event = ApnmtTestUtil.createClosingTimeEvent(ApnmtEventType.closingTimeDeleted);
        ClosingTime closingTime = this.closingTimeEventMapper.toEntity(event.getValue());
        this.closingTimeRepository.save(closingTime);

        int databaseSizeBeforeCreate = this.closingTimeRepository.findAll().size();

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.CLOSING_TIME_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<ClosingTime> closingTimes = this.closingTimeRepository.findAll();
        assertThat(closingTimes).hasSize(databaseSizeBeforeCreate - 1);
    }
}
