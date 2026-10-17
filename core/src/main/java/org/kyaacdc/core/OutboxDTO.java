package org.kyaacdc.core;

public record OutboxDTO (
        Long id,
        String payload,
        OutboxType type
) {}
