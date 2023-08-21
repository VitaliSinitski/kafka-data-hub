package com.example.gatewayservice.service.producer;

import com.example.gatewayservice.config.TopicProperties;
import com.example.gatewayservice.domain.Article;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Producer {
    private final TopicProperties topicProperties;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public String sendMessage(Article article) throws JsonProcessingException {
        String articleAsMessage = objectMapper.writeValueAsString(article);
        kafkaTemplate.send(topicProperties.getName(), articleAsMessage);
        log.info("article docs produced: {}", articleAsMessage);
        return "message sent";
    }
}
