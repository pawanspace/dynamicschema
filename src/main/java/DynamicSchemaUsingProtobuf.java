import com.google.protobuf.util.JsonFormat;
import com.itspawan.protobuf.DynamicJoinSchema;
import com.itspawan.protobuf.DynamicJoinSchema.DynamicSchema.JoinCondition;
import com.itspawan.protobuf.DynamicJoinSchema.DynamicSchema.AndCondition;
import com.itspawan.protobuf.DynamicJoinSchema.DynamicSchema.OrCondition;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static java.util.Arrays.asList;

public class DynamicSchemaUsingProtobuf {

    public static void main(String[] args) throws IOException {
        JoinCondition condition1 = JoinCondition.newBuilder()
                .setLhsColumn("replacementPartNumber")
                .setRhsColumn("part_number")
                .setOperation(DynamicJoinSchema.DynamicSchema.Operation.EQUAL).build();

        JoinCondition condition2 = JoinCondition.newBuilder()
                .setLhsColumn("brand")
                .setRhsColumn("brand_name")
                .setOperation(DynamicJoinSchema.DynamicSchema.Operation.NOT_EQUAL).build();


        AndCondition andCondition = AndCondition.newBuilder().addConditions(condition1).build();
        OrCondition orCondition = OrCondition.newBuilder().addConditions(condition2).build();


        DynamicJoinSchema.DynamicSchema schema = DynamicJoinSchema.DynamicSchema.newBuilder()
                .addAllLhsColumns(asList("replacementPartNumber", "brand", "wholegoodmodel"))
                .addAllRhsColumns(asList("part_number", "brand_name", "asin"))
                .addAllSelectableColumns(asList("replacementPartNumber", "brand", "wholegoodmodel", "asin"))
                .setAndCondition(andCondition)
                .setOrCondition(orCondition)
                .build();



        JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields();

        System.out.println(printer.print(schema));

        DynamicJoinSchema.DynamicSchema.Builder newBuilder = DynamicJoinSchema.DynamicSchema.newBuilder();
        JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
        File file = new File("/Users/pawanc/GitHub/spark-dynamic-join/proto.json");

        parser.merge(new FileReader(file), newBuilder);

      //  System.out.println(printer.print(newBuilder.build()));
    }
}
