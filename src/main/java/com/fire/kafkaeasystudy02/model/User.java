package com.fire.kafkaeasystudy02.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class User {
    private int id;

    private String phone;

    private Date birthday;
}
