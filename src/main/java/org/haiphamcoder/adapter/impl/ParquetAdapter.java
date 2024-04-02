package org.haiphamcoder.adapter.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.haiphamcoder.adapter.IParquetAdapter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ParquetAdapter implements IParquetAdapter {
    @Override
    public void writeParquetFile(List<Group> groups, String fileUri, MessageType schema) {
        try {
            File file = new File(fileUri);
            if (file.exists()) {
                file.delete();
            }

            Path path = new Path(fileUri);
            Configuration conf = new Configuration();
            conf.set("\"parquet.enable.summary-metadata", "false");
            final GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
            GroupWriteSupport.setSchema(schema, conf);

            class ParquetWriterBuilder extends ParquetWriter.Builder<Group, ParquetWriterBuilder> {

                protected ParquetWriterBuilder(Path path) {
                    super(path);
                }

                @Override
                protected ParquetWriterBuilder self() {
                    return this;
                }

                @Override
                protected WriteSupport<Group> getWriteSupport(Configuration configuration) {
                    return groupWriteSupport;
                }
            }

            ParquetWriter<Group> writer = new ParquetWriterBuilder(path)
                    .enableDictionaryEncoding()
                    .enableValidation()
                    .withCompressionCodec(ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withDictionaryPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                    .withConf(conf)
                    .build();
            for (Group group : groups) {
                writer.write(group);
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Group> readParquetFile(String fileUri) {
        List<Group> groups = new ArrayList<>();

        try {
            Path path = new Path(fileUri);
            if (!path.getFileSystem(new Configuration()).exists(path)) {
                throw new IllegalArgumentException("File not found: " + fileUri);
            }

            GroupReadSupport groupReadSupport = new GroupReadSupport();
            ParquetReader<Group> reader = ParquetReader.builder(groupReadSupport, path).build();

            Group group;
            while ((group = reader.read()) != null) {
                groups.add(group);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return groups;
    }
}
