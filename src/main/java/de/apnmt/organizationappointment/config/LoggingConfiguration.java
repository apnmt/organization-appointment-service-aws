package de.apnmt.organizationappointment.config;

import ch.qos.logback.classic.LoggerContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.apnmt.aws.common.config.AwsCloudProperties;
import de.apnmt.aws.common.config.logging.AwsLoggingUtils;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import tech.jhipster.config.JHipsterProperties;

import java.util.HashMap;
import java.util.Map;

import static tech.jhipster.config.logging.LoggingUtils.addContextListener;
import static tech.jhipster.config.logging.LoggingUtils.addJsonConsoleAppender;

/*
 * Configures the console and CloudWatch log appenders from the app properties
 */
@Configuration
public class LoggingConfiguration {

    public LoggingConfiguration(@Value("${spring.application.name}") String appName, @Value("${server.port}") String serverPort,
                                @Value("${cloud.aws.region.static}") String region, JHipsterProperties jHipsterProperties, AwsCloudProperties awsCloudProperties,
                                ObjectMapper mapper) throws JsonProcessingException {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

        Map<String, String> map = new HashMap<>();
        map.put("app_name", appName);
        map.put("app_port", serverPort);
        String customFields = mapper.writeValueAsString(map);

        JHipsterProperties.Logging loggingProperties = jHipsterProperties.getLogging();
        JHipsterProperties.Logging.Logstash logstashProperties = loggingProperties.getLogstash();

        if (loggingProperties.isUseJsonFormat()) {
            addJsonConsoleAppender(context, customFields);
        }
        if (awsCloudProperties.getLogging().getCloudWatch().isEnabled()) {
            AwsLoggingUtils.addCloudWatchAppender(context, mapper, appName, region, awsCloudProperties);
        }
        if (loggingProperties.isUseJsonFormat() || logstashProperties.isEnabled()) {
            addContextListener(context, customFields, loggingProperties);
        }
    }
}
