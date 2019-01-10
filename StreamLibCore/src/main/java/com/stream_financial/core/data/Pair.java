/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.data;

public class Pair<T, R> implements Comparable<Pair<T, R>> {

    public final T first;
    public final R second;

    private Pair(T first, R second) {
        this.first = first;
        this.second = second;
    }

    public static <T, R> Pair<T, R> of (T first, R second) {
        return new Pair<T, R>(first, second);
    }

    @Override
    public int compareTo(Pair<T, R> o) {
        int cmp = compare(first, o.first);
        return cmp == 0 ? compare(second, o.second) : cmp;
    }

    private static int compare(Object o1, Object o2) {
        return o1 == null ? o2 == null ? 0 : -1 : o2 == null ? +1 : ((Comparable) o1).compareTo(o2);
    }

    public int hashCode() {
        return 31 * hashCode(first) + hashCode(second);
    }

    private static int hashCode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;

        if (this == obj)
            return true;

        return equal(first, ((Pair) obj).first) && equal(second, ((Pair) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == null ? o2 == null : (o1 == o2 || o1.equals(o2));
    }

    public String toString() {
        return "(" + first + ", " + second + ")";
    }

}
