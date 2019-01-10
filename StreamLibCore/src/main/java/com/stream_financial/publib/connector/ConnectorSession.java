/* This file is proprietary software and may not reader any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 *
 * Created by Jian Dong on 23/04/2018.
 */

package com.stream_financial.publib.connector;

import com.stream_financial.core.StreamException;
import com.stream_financial.core.data.DataType;
import com.stream_financial.core.data.Pair;
import com.stream_financial.core.serialization.*;
import com.stream_financial.publib.serialization.PublicBinaryReader;
import com.stream_financial.publib.serialization.PublicBinaryWriter;
import com.stream_financial.publib.serialization.DefaultPublicBinaryReader;
import com.stream_financial.publib.serialization.DefaultPublicBinaryWriter;
import com.stream_financial.publib.table.DFColumn;
import com.stream_financial.publib.table.DFHeader;
import com.stream_financial.publib.table.DFTable;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ConnectorSession {
    private final Socket socket;
    private final PublicBinaryReader reader;
    private final PublicBinaryWriter writer;
    private final String clientName;

    private final static int DEFAULT_BUFFER_SIZE = 1048576;

    public ConnectorSession(String host, int port) {
        this(host, port, "JavaClient");
    }

    public ConnectorSession(String host, int port, String clientName) {
        try {
            this.socket = new Socket(host, port);
        } catch (IOException e) {
            throw new RuntimeException("ERROR: Failed to create socket.", e);
        }

        this.reader = new DefaultPublicBinaryReader<>(new SocketStreamReader(DEFAULT_BUFFER_SIZE, socket), true);
        this.writer = new DefaultPublicBinaryWriter<>(new SocketStreamWriter(DEFAULT_BUFFER_SIZE, socket, Endian.Little), true);
        this.clientName = clientName;
    }

    public Socket socket() {
        return this.socket;
    }

    public PublicBinaryReader reader() {
        return this.reader;
    }

    public PublicBinaryWriter writer() {
        return this.writer;
    }

    public Object readValue() {
        return reader.readValue();
    }

    public DFHeader readHeader() {
        DFHeader header = new DFHeader();

        int colCount = reader.readInt();

        for (int i=0; i < colCount; i++) {
            Pair<String, DataType> colHeader = Pair.of(reader.readString(), DataType.valueOf(reader.readInt()));
            header.add(colHeader);
        }

        return header;
    }

    public DFColumn readColumn(String name, DataType type, Long rowCount) {
        DFColumn column = new DFColumn(name, type);

        for (int i=0; i < rowCount; i++) {
            column.add(readValue());
        }

        return column;
    }

    public DFTable readTable() {
        DFHeader header = this.readHeader();
        long rowCount = reader.readLong();
        DFTable table = new DFTable("QueryResult");

        for (Pair<String, DataType> colHeader : header) {
            DFColumn column = this.readColumn(colHeader.first, colHeader.second, rowCount);
            table.add(column);
        }

        return table;
    }

    public void writeValue(Object value) {
        writer.write(value, false);
    }

    public void writeValue(Object value, boolean writeDataType) {
        writer.write(value, writeDataType, null);
    }

    public void writeValue(Object value, boolean writeDataType, DataType type) {
        writer.write(value, writeDataType, type);
    }

    public void writeMap(Map<String, Object> value) {
        writer.write(value);
    }

    public void writeHeader(DFHeader header) {
        for (Pair<String, DataType> columnHeader : header) {
                writeValue(columnHeader.first);
                writeValue(columnHeader.second.value());
        }
    }

    public void writeColumn(DFColumn column) {
        for (Object value : column) {
            writeValue(value, true, column.type());
        }
    }

    public void writeTable(DFTable table) {
        int colCount = table.size();
        long rowCount = table.get(0).size();

        // Table name
        writeValue(table.name());
        commit();

        // Header information
        writeValue(colCount);
        System.out.println("Writing header information:");
        for (DFColumn column : table) {
            this.writeValue(column.name());
            this.writeValue(column.type().value());
            System.out.println(column.name() + ":" + column.type().value());
        }
        commit();

        // Data
        writeValue(rowCount);
        for (DFColumn column : table) {
            System.out.println("Writing column " + column.name());
            this.writeColumn(column);
            commit();
        }
    }

    public String uploadTable(DFTable table) {
        // Client information
        Map<String, Object> clientInformation = new HashMap<>();
        clientInformation.put("clientName", clientName);
        writeMap(clientInformation);

        // Call function
        writeValue("uploadTable"); // Function
        Map<String, Object> functionInformation = new HashMap<>();
        writeMap(functionInformation);
        commit();

        // Write table
        writeTable(table);

        // Read return info
        byte status = reader.readByte();
        String returnMessage = reader.readString();

        return returnMessage;
    }

    public DFTable runQuery(String query){
        // Client information
        Map<String, Object> clientInformation = new HashMap<>();
        clientInformation.put("clientName", clientName);
        writeMap(clientInformation);

        // Function information
        writeValue("callQuery");
        Map<String, Object> functionInformation = new HashMap<>();
        writeMap(functionInformation);
        commit();

        writeValue(query);
        commit();

        byte resultOrException = reader.readByte();

        if (resultOrException == 0) {
            String exception = reader.readString();
            throw new StreamException("Error executing query: %s", exception);
        }

        return readTable();
    }

    public void registerProvider(Map<String, Object> providerInformation, Function<String, List<DFTable>> executeQuery) {
        // Client information
        Map<String, Object> clientInformation = new HashMap<>();
        clientInformation.put("clientName", clientName);
        writeMap(clientInformation);

        // Function information
        writeValue("registerExternalProvider");
        writeMap(providerInformation);
        commit();

        int returnStatus = reader.readInt();
        String returnMessage = reader.readString();

        if (returnStatus == 0) {
            throw new StreamException("Error registering provider: " + returnMessage);
        } else {
            System.out.print(returnMessage);
        }

        while (true) {
            String query = reader.readString();
            System.out.print("Query received: " + query);

            List<DFTable> resultTables = executeQuery.apply("");
            int numTables = resultTables.size();
            System.out.print("Query executed, return " + numTables + " tables.");
            writer.write(numTables);
            writer.commit();

            for (DFTable table : resultTables) {
                System.out.print("Sending table: " + table.name());
                writeTable(table);
            }

            System.out.print("Tables sent.");
        }
    }

    public void commit() {
        writer.commit();
    }

    public void close() {
        try {
            System.out.printf("Closing socket connection to %s...\n", socket.toString());
            socket.close();
        } catch (IOException e) {
            throw new RuntimeException("ERROR: Failed to close socket connection.");
        }
    }
}
