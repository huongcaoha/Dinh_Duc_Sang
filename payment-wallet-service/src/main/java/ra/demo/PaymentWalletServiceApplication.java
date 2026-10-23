package ra.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@EnableDiscoveryClient
@SpringBootApplication
public class PaymentWalletServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentWalletServiceApplication.class, args);
    }

}
