package org.haiphamcoder;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.haiphamcoder.adapter.IParquetAdapter;
import org.haiphamcoder.adapter.impl.ParquetAdapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        String schemaString = "message Car {\n" +
                "  required binary make (UTF8);\n" +
                "  required int32 year;\n" +
                "\n" +
                "  repeated group part {\n" +
                "     required binary name (UTF8);\n" +
                "     optional int32 life;\n" +
                "     repeated binary oem (UTF8);\n" +
                "  }\n" +
                "}";

        MessageType schema = MessageTypeParser.parseMessageType(schemaString);

        GroupFactory groupFactory = new SimpleGroupFactory(schema);

        Group porsche = groupFactory.newGroup()
                .append("make", "Porsche")
                .append("year", 2025);
        porsche.addGroup("part")
                .append("name", "Tyre");


        Group bmw = groupFactory.newGroup()
                .append("make", "BMW")
                .append("year", 2019);

        List<Group> groups = new ArrayList<>();
        Collections.addAll(groups, porsche, bmw);

        String fileUri = "src/main/resources/data/cars.parquet";
        IParquetAdapter parquetAdapter = new ParquetAdapter();
        parquetAdapter.writeParquetFile(groups, fileUri, schema);

        List<Group> readGroups = parquetAdapter.readParquetFile(fileUri);
        for (Group group : readGroups) {
            System.out.println(group);
        }
    }
}