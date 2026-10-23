package ra.demo.dto.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DriverLocationStreamDTO {
    private Long driverId;
    private Long tripId;
    private Double lat;
    private Double lng;
    private String vehicleType;
}