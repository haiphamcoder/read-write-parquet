package org.haiphamcoder.adapter;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;

import java.util.List;

public interface IParquetAdapter {
    void writeParquetFile(List<Group> groups, String fileUri, MessageType schema);

    List<Group> readParquetFile(String fileUri);
}
