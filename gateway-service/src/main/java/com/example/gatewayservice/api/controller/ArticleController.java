package com.example.gatewayservice.api.controller;

import com.example.gatewayservice.api.request.ArticleRequestDto;
import com.example.gatewayservice.service.ArticleService;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/article")
@RequiredArgsConstructor
@Validated
public class ArticleController {
    private final ArticleService articleService;

    @PostMapping
    public ResponseEntity<String> createArticle(
            @RequestBody @Valid ArticleRequestDto articleRequestDto) throws JsonProcessingException {
        String article = articleService.createArticle(articleRequestDto);
        return ResponseEntity.ok(article);
    }
}
