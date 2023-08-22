package com.example.gatewayservice.service;

import com.example.gatewayservice.api.controller.ArticleController;
import com.example.gatewayservice.api.converter.ArticleConverter;
import com.example.gatewayservice.api.request.ArticleRequestDto;
import com.example.gatewayservice.domain.Article;
import com.example.gatewayservice.service.producer.Producer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ArticleService {
    private final Producer producer;
    private final ArticleConverter articleConverter;
    public String createArticle(ArticleRequestDto articleRequestDto) throws JsonProcessingException {
        Article article = articleConverter.convertToArticleArticleRequestDto(articleRequestDto);
        return producer.sendMessage(article);
    }

}
