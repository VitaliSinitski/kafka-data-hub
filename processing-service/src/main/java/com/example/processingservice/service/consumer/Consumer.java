package com.example.processingservice.service.consumer;

import com.example.processingservice.domain.Article;
import com.example.processingservice.domain.dto.ArticleDto;
import com.example.processingservice.service.ArticleService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Consumer {
    private static final String orderTopic = "${topic.name}";
    private final ModelMapper modelMapper;
    private final ObjectMapper objectMapper;
    private final ArticleService articleService;

    @KafkaListener(topics = orderTopic)
    public void consumeMessage(String message) {
        try {
            log.info("message consumed {}", message);

            ArticleDto articleDto = objectMapper.readValue(message, ArticleDto.class);
            Article article = modelMapper.map(articleDto, Article.class);

            articleService.persistArticle(article);
        } catch (JsonProcessingException ex) {
            log.error("Error processing JSON from Kafka message.", ex);
        } catch (Exception ex) {
            log.error("Error processing Kafka message.", ex);
        }
    }
}
