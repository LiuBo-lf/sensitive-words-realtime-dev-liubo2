package com.lb.dws.uitil;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))

public  class UdtfTest extends TableFunction<Row> {

    public void eval(String str) {

        List<String> strings = IkTest.ik(str);
        for (String s : strings) {
            collect(Row.of(s));
        }
    }
}
