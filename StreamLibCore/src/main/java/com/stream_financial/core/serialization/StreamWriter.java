/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.serialization;

public interface StreamWriter<C>
{
    Endian endian();
    void setContext(C context);
    void write(byte[] raw, int offset, int bytes);
    void commit();
}