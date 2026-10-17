package org.kyaacdc.employeeservice.controller;

import lombok.AllArgsConstructor;
import org.kyaacdc.employeeservice.service.EmployeeService;
import org.kyaacdc.employeeservice.service.dto.CreateEmployeeDto;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;

@RestController
@RequestMapping("/employees")
@AllArgsConstructor
public class EmployeeController {

    private final EmployeeService employeeService;

    @PostMapping
    public ResponseEntity<Object> createEmployee(@RequestBody CreateEmployeeDto createEmployeeDto)  {


        CreateEmployeeDto pEmployee;

        try {
            pEmployee = employeeService.createPEmployee(createEmployeeDto);
        } catch (Exception e) {
            return ResponseEntity.status(INTERNAL_SERVER_ERROR).body(e.getMessage());
        }

        return ResponseEntity.status(CREATED).body(pEmployee);
    }
}
