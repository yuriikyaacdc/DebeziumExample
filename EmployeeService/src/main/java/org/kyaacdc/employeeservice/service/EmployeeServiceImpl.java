package org.kyaacdc.employeeservice.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kyaacdc.core.OutboxType;
import org.kyaacdc.employeeservice.persistence.EmployeeEntity;
import org.kyaacdc.employeeservice.persistence.EmployeeRepository;
import org.kyaacdc.employeeservice.persistence.OutboxEntity;
import org.kyaacdc.employeeservice.persistence.OutboxRepository;
import org.kyaacdc.employeeservice.service.dto.CreateEmployeeDto;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Service
@AllArgsConstructor
@Slf4j
public class EmployeeServiceImpl implements EmployeeService {

    private final EmployeeRepository employeeRepository;
    private final OutboxRepository outboxRepository;
    private ObjectMapper objectMapper;

    @Override
    @Transactional
    public CreateEmployeeDto createPEmployee(CreateEmployeeDto createEmployeeDto) {

        log.info("Creating employee {}", createEmployeeDto);

        EmployeeEntity employeeEntity = new EmployeeEntity();

        BeanUtils.copyProperties(createEmployeeDto, employeeEntity);

        employeeEntity = employeeRepository.save(employeeEntity);

        OutboxEntity outboxEntity = new OutboxEntity();
        String employeeAsString = objectMapper.writeValueAsString(employeeEntity);
        outboxEntity.setType(OutboxType.CV.name());
        outboxEntity.setPayload(employeeAsString);
        outboxRepository.save(outboxEntity);

        return createEmployeeDto;
    }
}
