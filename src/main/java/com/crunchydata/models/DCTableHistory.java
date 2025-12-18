package com.crunchydata.models;

import lombok.Data;

import javax.json.Json;
import java.time.OffsetDateTime;

@Data
public class DCTableHistory {
    private int tid;
    private String loadId;
    private Integer batchNbr = 1;
    private OffsetDateTime startDt;
    private OffsetDateTime endDt;
    private Json actionResult;
    private String actionType;
    private int rowCount;
}
