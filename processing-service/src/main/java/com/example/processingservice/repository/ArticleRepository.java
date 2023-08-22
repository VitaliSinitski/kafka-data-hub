package com.example.processingservice.repository;

import com.example.processingservice.domain.Article;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ArticleRepository extends MongoRepository<Article, String> {
}
