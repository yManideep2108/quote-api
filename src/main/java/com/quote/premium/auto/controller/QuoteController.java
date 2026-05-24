package com.quote.premium.auto.controller;

import com.quote.premium.auto.dto.QuoteRequest;
import com.quote.premium.auto.dto.QuoteResponse;
import com.quote.premium.auto.service.QuoteService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/quote")
public class QuoteController {
    @Autowired
    private QuoteService quoteService;
    @PostMapping
    public QuoteResponse getQuote(@RequestBody QuoteRequest request) {
        return quoteService.getQuote(request.getVins());
    }
}