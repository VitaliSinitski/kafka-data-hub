package com.example.gatewayservice.api.request;

import jakarta.validation.constraints.NotBlank;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ArticleRequestDto {
    @NotBlank(message = "This field is required")
    private String title;
    @NotBlank(message = "This field is required")
    private String author;
    private String description;
}
