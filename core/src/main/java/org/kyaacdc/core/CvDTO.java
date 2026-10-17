package org.kyaacdc.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class CvDTO {

    Long id;
    String name;
    String education;
    String description;
}
