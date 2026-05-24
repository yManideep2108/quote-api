package com.quote.premium.auto.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class QuoteEvent {
    private Long policyId ;
    private String customerName;
    private List<String> vins;
}
