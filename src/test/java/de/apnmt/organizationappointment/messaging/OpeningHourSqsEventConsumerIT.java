package de.apnmt.organizationappointment.messaging;

import java.io.IOException;
import java.util.List;

import de.apnmt.aws.common.test.AbstractEventConsumerIT;
import de.apnmt.common.ApnmtTestUtil;
import de.apnmt.common.TopicConstants;
import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.ApnmtEventType;
import de.apnmt.common.event.value.OpeningHourEventDTO;
import de.apnmt.organizationappointment.common.domain.OpeningHour;
import de.apnmt.organizationappointment.common.repository.OpeningHourRepository;
import de.apnmt.organizationappointment.common.service.mapper.OpeningHourEventMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class OpeningHourSqsEventConsumerIT extends AbstractEventConsumerIT {

    @Autowired
    private OpeningHourRepository openingHourRepository;

    @Autowired
    private OpeningHourEventMapper openingHourEventMapper;

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        AbstractEventConsumerIT.beforeAll(TopicConstants.OPENING_HOUR_CHANGED_TOPIC, QueueConstants.OPENING_HOUR_QUEUE);
    }

    @BeforeEach
    public void initTest() {
        this.openingHourRepository.deleteAll();
    }

    @Test
    void openingHourCreatedTest() throws InterruptedException {
        int databaseSizeBeforeCreate = this.openingHourRepository.findAll().size();
        ApnmtEvent<OpeningHourEventDTO> event = ApnmtTestUtil.createOpeningHourEvent(ApnmtEventType.openingHourCreated);

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.OPENING_HOUR_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<OpeningHour> openingHours = this.openingHourRepository.findAll();
        assertThat(openingHours).hasSize(databaseSizeBeforeCreate + 1);
        OpeningHour openingHour = openingHours.get(openingHours.size() - 1);
        OpeningHourEventDTO openingHourEventDTO = event.getValue();
        assertThat(openingHour.getId()).isEqualTo(openingHourEventDTO.getId());
        assertThat(openingHour.getStartTime()).isEqualTo(openingHourEventDTO.getStartTime());
        assertThat(openingHour.getEndTime()).isEqualTo(openingHourEventDTO.getEndTime());
        assertThat(openingHour.getDay()).isEqualTo(openingHourEventDTO.getDay());
        assertThat(openingHour.getOrganizationId()).isEqualTo(openingHourEventDTO.getOrganizationId());
    }

    @Test
    void openingHourUpdatedTest() throws InterruptedException {
        OpeningHourEventDTO eventDTO = ApnmtTestUtil.createOpeningHourEventDTO();
        OpeningHour oh = this.openingHourEventMapper.toEntity(eventDTO);
        this.openingHourRepository.save(oh);

        int databaseSizeBeforeCreate = this.openingHourRepository.findAll().size();
        ApnmtEvent<OpeningHourEventDTO> event = ApnmtTestUtil.createOpeningHourEvent(ApnmtEventType.openingHourCreated);
        event.getValue().setOrganizationId(10L);

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.OPENING_HOUR_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<OpeningHour> openingHours = this.openingHourRepository.findAll();
        assertThat(openingHours).hasSize(databaseSizeBeforeCreate);
        OpeningHour openingHour = openingHours.get(openingHours.size() - 1);
        OpeningHourEventDTO openingHourEventDTO = event.getValue();
        assertThat(openingHour.getId()).isEqualTo(openingHourEventDTO.getId());
        assertThat(openingHour.getStartTime()).isEqualTo(openingHourEventDTO.getStartTime());
        assertThat(openingHour.getEndTime()).isEqualTo(openingHourEventDTO.getEndTime());
        assertThat(openingHour.getDay()).isEqualTo(openingHourEventDTO.getDay());
        assertThat(openingHour.getOrganizationId()).isNotEqualTo(eventDTO.getOrganizationId());
        assertThat(openingHour.getOrganizationId()).isEqualTo(openingHourEventDTO.getOrganizationId());
    }

    @Test
    void openingHourDeletedTest() throws InterruptedException {
        ApnmtEvent<OpeningHourEventDTO> event = ApnmtTestUtil.createOpeningHourEvent(ApnmtEventType.openingHourDeleted);
        OpeningHour openingHour = this.openingHourEventMapper.toEntity(event.getValue());
        this.openingHourRepository.save(openingHour);

        int databaseSizeBeforeCreate = this.openingHourRepository.findAll().size();

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.OPENING_HOUR_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<OpeningHour> openingHours = this.openingHourRepository.findAll();
        assertThat(openingHours).hasSize(databaseSizeBeforeCreate - 1);
    }
}
