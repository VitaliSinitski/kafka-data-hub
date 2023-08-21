package com.example.gatewayservice.api.converter;

import com.example.gatewayservice.api.request.ArticleRequestDto;
import com.example.gatewayservice.domain.Article;
import org.springframework.stereotype.Component;

@Component
public class ArticleConverter {
    public Article convertToArticleArticleRequestDto(ArticleRequestDto articleRequestDto) {
        return Article.builder()
                .author(articleRequestDto.getAuthor())
                .title(articleRequestDto.getTitle())
                .description(articleRequestDto.getDescription())
                .build();

    }
}
