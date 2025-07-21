import argparse

import pandas as pd
from sqlalchemy import create_engine


def convert_file(
    input_file,
    output_file,
    input_format,
    output_format,
    sql_table=None,
    db_uri=None,
):
    # Read the input file
    if input_format == "csv":
        df = pd.read_csv(input_file)
    elif input_format == "excel":
        df = pd.read_excel(input_file)
    elif input_format == "json":
        df = pd.read_json(input_file)
    elif input_format == "sql":
        if not db_uri or not sql_table:
            raise ValueError("For SQL input, both db_uri and sql_table are required.")
        engine = create_engine(db_uri)
        df = pd.read_sql(sql_table, engine)
    else:
        raise ValueError(f"Unsupported input format: {input_format}")

    # Write to the output file
    if output_format == "csv":
        df.to_csv(output_file, index=False)
    elif output_format == "excel":
        df.to_excel(output_file, index=False)
    elif output_format == "json":
        df.to_json(output_file, orient="records")
    elif output_format == "sql":
        if not db_uri or not sql_table:
            raise ValueError("For SQL output, both db_uri and sql_table are required.")
        engine = create_engine(db_uri)
        df.to_sql(sql_table, engine, if_exists="replace", index=False)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")

    print(f"File converted from {input_format} to {output_format} and saved to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Convert data files between CSV, Excel, JSON, and SQL formats.")
    parser.add_argument("input_file", help="Path to the input file")
    parser.add_argument("output_file", help="Path to the output file")
    parser.add_argument(
        "input_format",
        choices=["csv", "excel", "json", "sql"],
        help="Format of the input file",
    )
    parser.add_argument(
        "output_format",
        choices=["csv", "excel", "json", "sql"],
        help="Format of the output file",
    )
    parser.add_argument("--sql_table", help="Table name for SQL input/output")
    parser.add_argument("--db_uri", help="Database URI for SQL input/output")

    args = parser.parse_args()

    convert_file(
        input_file=args.input_file,
        output_file=args.output_file,
        input_format=args.input_format,
        output_format=args.output_format,
        sql_table=args.sql_table,
        db_uri=args.db_uri,
    )


# if __name__ == "__main__":
#    main()
