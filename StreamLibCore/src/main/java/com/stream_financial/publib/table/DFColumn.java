/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 *
 * Created by Jian Dong on 23/04/2018.
 */

package com.stream_financial.publib.table;

import com.stream_financial.publib.connector.ConnectorSession;
import com.stream_financial.core.data.DataType;

import java.util.ArrayList;

public class DFColumn<T> extends ArrayList<T> {
    private String name;
    private DataType type;

    public DFColumn(String name, DataType type){
        this.name = name;
        this.type = type;
    }

    public void writeColumn(ConnectorSession session) {
        session.writeColumn(this);
    }

    public String name() {
        return this.name;
    }

    public DataType type() {
        return this.type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(DataType type) {
        this.type = type;
    }
}
