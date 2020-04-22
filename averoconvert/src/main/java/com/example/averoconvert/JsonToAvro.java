package com.example.averoconvert;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.springframework.stereotype.Service;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sound.midi.Soundbank;

@RestController
public class JsonToAvro {


    @GetMapping("/jsontoavro")
    public String jsonAvroConvert() throws IOException {

        String schemaStr = "{\n" +
                "  \"name\": \"MyClass\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"namespace\": \"com.acme.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"name\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"age\",\n" +
                "      \"type\": \"int\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String json = "{" +
        "\"name\":\"Gopi \"," +
        "\"age\":38" +
        "}";

        jsonToAvro(json, schemaStr);
        byte[] averoMessage = jsonToAvro(json, schemaStr);
        System.out.println("JSON to Avro message ----" + averoMessage);
        System.out.println("Avro to JSON message ----" + avroToJson(averoMessage));
        return  "JSON to Avro message ----" + averoMessage + "\n" +
                "Avro to JSON message ----" + avroToJson(averoMessage);
    }


    public byte[] jsonToAvro(String json, String schemaStr) throws IOException {
        InputStream input = null;
        DataFileWriter<GenericRecord> writer = null;
        Encoder encoder = null;
        ByteArrayOutputStream output = null;
        try {
            Schema schema = new Schema.Parser().parse(schemaStr);
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            input = new ByteArrayInputStream(json.getBytes());
            output = new ByteArrayOutputStream();
            DataInputStream din = new DataInputStream(input);
            writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>());
            writer.create(schema, output);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            GenericRecord datum;
            while (true) {
                try {
                    datum = reader.read(null, decoder);
                } catch (EOFException eofe) {
                    break;
                }
                writer.append(datum);
            }
            writer.flush();
            return output.toByteArray();
        } finally {
            try { input.close(); } catch (Exception e) { }
        }
    }

    @GetMapping("/avrotojson")
    public static String avroToJson(byte[] avro) throws IOException {
        boolean pretty = false;
        GenericDatumReader<GenericRecord> reader = null;
        JsonEncoder encoder = null;
        ByteArrayOutputStream output = null;
        try {
            reader = new GenericDatumReader<GenericRecord>();
            InputStream input = new ByteArrayInputStream(avro);
            DataFileStream<GenericRecord> streamReader = new DataFileStream<GenericRecord>(input, reader);
            output = new ByteArrayOutputStream();
            Schema schema = streamReader.getSchema();
            System.out.println("Schema-----" + schema.toString());
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            encoder = EncoderFactory.get().jsonEncoder(schema, output, pretty);
            for (GenericRecord datum : streamReader) {
                writer.write(datum, encoder);
            }
            encoder.flush();
            output.flush();
            return new String(output.toByteArray());
        } finally {
            try { if (output != null) output.close(); } catch (Exception e) { }
        }
    }
}
