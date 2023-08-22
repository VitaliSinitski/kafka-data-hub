package com.example.processingservice.service;

import com.example.processingservice.domain.Article;
import com.example.processingservice.repository.ArticleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ArticleService {
    private final ArticleRepository articleRepository;

    public void persistArticle(Article article) {
        Article persistedArticle = articleRepository.save(article);
        log.info("article document persisted {}", persistedArticle);
    }
}
