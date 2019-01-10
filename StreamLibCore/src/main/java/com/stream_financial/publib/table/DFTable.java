/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 *
 * Created by Jian Dong on 23/04/2018.
 */

package com.stream_financial.publib.table;

import com.stream_financial.publib.connector.ConnectorSession;

import java.util.ArrayList;

public class DFTable extends ArrayList<DFColumn> {
    private String name;

    public DFTable(String name) {
        this.name = name;
    }

    public String upload(ConnectorSession session) {
        return session.uploadTable(this);
    }

    public String name() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }
}