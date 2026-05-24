package com.quote.premium.auto.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class QuoteResponse {
     private Double totalPremium ;
     private QuoteStatus  status ;
}
