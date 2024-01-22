package com.se.flink.se.flink.handle.tableapi;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import java.time.LocalDate;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

public class TableAPI {

    public static void main(String[] args) {

        tableAPI();
    }

    public static void tableAPI() {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
//                .inStreamingMode()
                .inBatchMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        Table table = tableEnv.fromValues(
                DataTypes.ROW(DataTypes.FIELD("ID", DataTypes.INT()),
                        DataTypes.FIELD("NAME", DataTypes.STRING()),
                        DataTypes.FIELD("DOB", DataTypes.STRING())
                ),
                row(12L, "Alice", LocalDate.of(1984, 3, 12)),
                row(32L, "Bob", LocalDate.of(1990, 10, 14)),
                row(7L, "Kyle", LocalDate.of(1979, 2, 23)));
        Table counts = table.select($("ID"), $("NAME"), $("DOB"));
        System.out.println("Table value");
        counts.execute().print();
        Table where = counts.where($("ID").isEqual(12));
        System.out.println("Table where value");
        where.execute().print();

        System.out.println("==========================");
        Table table2 = tableEnv.fromValues(
                DataTypes.ROW(DataTypes.FIELD("ORDER_ID", DataTypes.INT()),
                        DataTypes.FIELD("PROD", DataTypes.STRING()),
                        DataTypes.FIELD("COLOR", DataTypes.STRING())
                ),
                row(12L, "Hat", "RED"),
                row(32L, "Shoe", "WHITE"),
                row(7L, "Coat", "BLUE"));
        Table select = table.join(table2).where(
                $("ID").isEqual($("ORDER_ID"))
        ).select($("ID"), $("NAME"), $("DOB"), $("COLOR"));
        select.execute().print();
    }
}
