package com.example.gatewayservice.domain;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Article {
    private String title;
    private String description;
    private String author;
}
