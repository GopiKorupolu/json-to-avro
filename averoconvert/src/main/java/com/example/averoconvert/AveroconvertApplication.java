package com.example.averoconvert;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class AveroconvertApplication {



	public static void main(String[] args) {
		SpringApplication.run(AveroconvertApplication.class, args);
		JsonToAvro jsonToAvro = new JsonToAvro();
		try {
			jsonToAvro.jsonAvroConvert();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
