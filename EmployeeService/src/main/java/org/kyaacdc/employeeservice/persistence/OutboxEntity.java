package org.kyaacdc.employeeservice.persistence;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.kyaacdc.core.OutboxType;

@Entity
@Table(name = "outbox")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OutboxEntity {

    @Id
    @GeneratedValue
    private long id;

    @Column
    private String payload;

    @Column
    private String type;
}
