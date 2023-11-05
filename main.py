from mylib.lib import (
    initialize_spark,
    read_csv,
    sql_query,
    perform_data_transformation,
    save_summary_report,
)


def main():
    # Step 1: Initializing the spark connection
    spark = initialize_spark()

    # Step 2: Load the songs_normalize dataset
    file_path = "dnd_chars_unique.tsv"
    songs_df = read_csv(spark, file_path)

    # Print out the DataFrame to verify if it's loaded correctly
    songs_df.show()

    # Step 3: Register DataFrame as a temporary SQL table/view
    songs_df.createOrReplaceTempView("justClass")

    # Step 4: Perform a Spark SQL query
    sql_query_string = "justClass = 'Human'"
    result_df = sql_query(songs_df, sql_query_string)

    # Print out the result DataFrame
    result_df.show()

    # Step 5: Perform data transformation
    column_of_interest = "level"
    target_value = "1"
    transformed_df = perform_data_transformation(
        songs_df, column_of_interest, target_value
    )

    # Combine the results into the report_content
    report_content = "SQL Query Result:\n"
    report_content += f"Number of rows: {result_df.count()}\n\n"
    report_content += "Transformed DataFrame:\n"
    report_content += f"Number of rows: {transformed_df.count()}\n"

    # Step 6: Save summary report
    report_file_path = "report.txt"
    save_summary_report(report_content, report_file_path)

    # Display the result DataFrame
    transformed_df.show()


if __name__ == "__main__":
    main()