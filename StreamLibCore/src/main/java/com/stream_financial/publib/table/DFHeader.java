package com.stream_financial.publib.table;

import com.stream_financial.publib.connector.ConnectorSession;
import com.stream_financial.core.data.DataType;
import com.stream_financial.core.data.Pair;

import java.util.ArrayList;

public class DFHeader extends ArrayList<Pair<String, DataType>> {
    public void write(ConnectorSession session) {
        session.writeHeader(this);
    }
}