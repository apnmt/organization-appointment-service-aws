package de.apnmt.organizationappointment.messaging;

import java.io.IOException;
import java.util.List;

import de.apnmt.aws.common.test.AbstractEventConsumerIT;
import de.apnmt.common.ApnmtTestUtil;
import de.apnmt.common.TopicConstants;
import de.apnmt.common.event.ApnmtEvent;
import de.apnmt.common.event.ApnmtEventType;
import de.apnmt.common.event.value.ServiceEventDTO;
import de.apnmt.organizationappointment.common.domain.Service;
import de.apnmt.organizationappointment.common.repository.ServiceRepository;
import de.apnmt.organizationappointment.common.service.mapper.ServiceEventMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class ServiceSqsEventConsumerIT extends AbstractEventConsumerIT {

    @Autowired
    private ServiceRepository serviceRepository;

    @Autowired
    private ServiceEventMapper serviceEventMapper;

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        AbstractEventConsumerIT.beforeAll(TopicConstants.SERVICE_CHANGED_TOPIC, QueueConstants.SERVICE_QUEUE);
    }

    @BeforeEach
    public void initTest() {
        this.serviceRepository.deleteAll();
    }

    @Test
    public void serviceCreatedTest() throws InterruptedException {
        int databaseSizeBeforeCreate = this.serviceRepository.findAll().size();
        ApnmtEvent<ServiceEventDTO> event = ApnmtTestUtil.createServiceEvent(ApnmtEventType.serviceCreated);

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.SERVICE_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<Service> services = this.serviceRepository.findAll();
        assertThat(services).hasSize(databaseSizeBeforeCreate + 1);
        Service service = services.get(services.size() - 1);
        ServiceEventDTO serviceEventDTO = event.getValue();
        assertThat(service.getId()).isEqualTo(serviceEventDTO.getId());
        assertThat(service.getDuration()).isEqualTo(serviceEventDTO.getDuration());
    }

    @Test
    public void serviceUpdatedTest() throws InterruptedException {
        ServiceEventDTO eventDTO = ApnmtTestUtil.createServiceEventDTO();
        Service s = this.serviceEventMapper.toEntity(eventDTO);
        this.serviceRepository.save(s);

        int databaseSizeBeforeCreate = this.serviceRepository.findAll().size();
        ApnmtEvent<ServiceEventDTO> event = ApnmtTestUtil.createServiceEvent(ApnmtEventType.serviceCreated);
        event.getValue().setDuration(100);

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.SERVICE_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<Service> services = this.serviceRepository.findAll();
        assertThat(services).hasSize(databaseSizeBeforeCreate);
        Service service = services.get(services.size() - 1);
        ServiceEventDTO serviceEventDTO = event.getValue();
        assertThat(service.getId()).isEqualTo(serviceEventDTO.getId());
        assertThat(service.getDuration()).isNotEqualTo(eventDTO.getDuration());
        assertThat(service.getDuration()).isEqualTo(serviceEventDTO.getDuration());
    }

    @Test
    public void serviceDeletedTest() throws InterruptedException {
        ApnmtEvent<ServiceEventDTO> event = ApnmtTestUtil.createServiceEvent(ApnmtEventType.serviceDeleted);
        Service service = this.serviceEventMapper.toEntity(event.getValue());
        this.serviceRepository.save(service);

        int databaseSizeBeforeCreate = this.serviceRepository.findAll().size();

        this.notificationMessagingTemplate.convertAndSend(TopicConstants.SERVICE_CHANGED_TOPIC, event);

        Thread.sleep(1000);

        List<Service> services = this.serviceRepository.findAll();
        assertThat(services).hasSize(databaseSizeBeforeCreate - 1);
    }
}
