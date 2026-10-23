package ra.demo.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import ra.demo.dto.response.ApiResponse;

import java.math.BigDecimal;

@FeignClient(name = "payment-wallet-service", url = "${application.config.payment-service-url:http://localhost:8084}")
public interface PaymentClient {

    @PostMapping("/api/v1/payments/hold")
    ApiResponse<Void> holdCustomerWallet(
            @RequestParam("customerId") Long customerId,
            @RequestParam("tripId") Long tripId,
            @RequestParam("fareAmount") BigDecimal fareAmount
    );
}
