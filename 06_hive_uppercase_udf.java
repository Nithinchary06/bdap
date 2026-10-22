// EXPERIMENT 6 — HIVE USER-DEFINED FUNCTION (UPPERCASE)
// Aim: Create a Hive UDF that converts text to uppercase.
// Compile against Hive/Hadoop libraries matching the lab installation.

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class UpperCaseUDF extends UDF {
    public Text evaluate(Text input) {
        // Return null for a null input; otherwise return uppercase text.
        if (input == null) return null;
        return new Text(input.toString().toUpperCase());
    }
}

/*
EXECUTION IN HIVE CLI / BEELINE AFTER BUILDING uppercase.jar:
  ADD JAR /absolute/path/uppercase.jar;
  CREATE TEMPORARY FUNCTION to_upper AS 'UpperCaseUDF';
  SELECT to_upper('big data lab');
  SELECT to_upper(customer_name) FROM customer;

EXPECTED CHECK:
  BIG DATA LAB

Notes:
- Replace the JAR path with the actual absolute path.
- Temporary functions last only for the current Hive session.
- If the Java class uses a package, register its fully qualified class name.
*/
