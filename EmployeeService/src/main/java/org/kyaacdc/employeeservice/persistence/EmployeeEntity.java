package org.kyaacdc.employeeservice.persistence;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "employee")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EmployeeEntity {

    @Id
    @GeneratedValue
    private long id;

    @Column
    private String name;

    @Column
    private String education;

    @Column
    private String description;

}
