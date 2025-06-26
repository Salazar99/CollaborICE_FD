import pandas as pd
import sys
import re # For regular expressions to parse the 'value' column
import json # Potentially for JSON parsing if 'value' or 'key' can be JSON

def manipulate_kafka_messages_general(filepath):
    """
    Loads and manipulates a CSV file representing Kafka messages, designed to be
    more general-purpose for files where all columns might contain values.

    Args:
        filepath (str): The path to your CSV file.

    Returns:
        pandas.DataFrame: The manipulated DataFrame.
    """
    try:
        # 1. Load the CSV file
        df = pd.read_csv(filepath)
        print(f"Successfully loaded '{filepath}'. Initial shape: {df.shape}")
        print("\nInitial DataFrame Head:")
        print(df.head().to_markdown(index=False, numalign="left", stralign="left"))

        # 2. Basic Data Inspection
        print("\nDataFrame Info:")
        df.info()

        # 3. Convert 'timestamp' (Unix milliseconds) to datetime
        # This assumes 'timestamp' is in milliseconds. Adjust 'unit' if it's seconds, microseconds, etc.
        # 'errors='coerce' will turn unparseable values into NaT (Not a Time)
        if 'timestamp' in df.columns and pd.api.types.is_numeric_dtype(df['timestamp']):
            print("\nConverting 'timestamp' to datetime...")
            df['timestamp_readable'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
            print("Converted 'timestamp_readable' column added.")
            print(df[['timestamp', 'timestamp_readable']].head().to_markdown(index=False, numalign="left", stralign="left"))
        else:
            print("\n'timestamp' column not found or not numeric, skipping conversion.")


        # 4. General Parsing for 'key' and 'value' columns
        # The 'key' and 'value' columns often contain byte strings (b'...')
        # or JSON strings. This function attempts to handle both.
        def decode_and_parse_kafka_data(data_str):
            if not isinstance(data_str, str):
                return data_str # Return as is if not a string (e.g., NaN, numeric)

            # Attempt to decode byte string
            if data_str.startswith("b'") and data_str.endswith("'"):
                decoded_str = data_str[2:-1].replace('\\\\', '\\')
            else:
                decoded_str = data_str

            # Attempt to parse as JSON
            try:
                return json.loads(decoded_str)
            except json.JSONDecodeError:
                # If not JSON, return the decoded string itself
                return decoded_str

        # Apply this general parsing for 'key' if it exists
        if 'key' in df.columns:
            print("\nAttempting to decode and parse 'key' column...")
            # For the specific file provided, 'key' is all NaN. This handles that gracefully.
            # If future files have actual keys, this will attempt to decode/parse them.
            df['key_parsed'] = df['key'].apply(decode_and_parse_kafka_data)
            print(df[['key', 'key_parsed']].head().to_markdown(index=False, numalign="left", stralign="left"))

        # Specific parsing for the 'value' column based on your provided file's format
        # This part is highly specific to the 'influxdb-like' format seen in your CSV.
        # If other files have different 'value' formats (e.g., pure JSON),
        # this function will need significant modification or replacement.
        def parse_value_influx_like(value_str):
            if not isinstance(value_str, str):
                return {} # Return empty dict for non-string types

            # Decode byte string if present (e.g., b'...')
            if value_str.startswith("b'") and value_str.endswith("'"):
                value_str = value_str[2:-1].replace('\\\\', '\\') # Remove b'' and handle escaped backslashes

            # Split by the first space after the key-value pairs, which separates fields from the final timestamp
            parts = value_str.rsplit(' ', 1)
            if len(parts) < 2: # Handle cases without a final timestamp or malformed
                data_part = parts[0]
                final_value_timestamp = None
            else:
                data_part = parts[0]
                try:
                    final_value_timestamp = int(parts[1]) # Attempt to convert the last part to an integer timestamp
                except ValueError:
                    final_value_timestamp = None # If not an integer, treat as part of data or skip

            # Split into measurement, tags, and fields
            data_elements = data_part.split(',')
            parsed_data = {}

            if data_elements:
                parsed_data['measurement'] = data_elements[0].strip() # First element as measurement

                # Process remaining elements as key=value pairs
                for item in data_elements[1:]:
                    if '=' in item:
                        key, val = item.split('=', 1)
                        parsed_data[key.strip()] = val.strip()

                # Extract metric name and value (e.g., voltage=227.663742)
                # This regex looks for 'word=number' at the end of the data part or followed by a space
                metric_match = re.search(r'([a-zA-Z_]+)=(\-?\d+\.?\d*)(?:$|\s)', data_part)
                if metric_match:
                    metric_name = metric_match.group(1)
                    try:
                        metric_value = float(metric_match.group(2))
                    except ValueError:
                        metric_value = None # Handle cases where value isn't a valid float

                    parsed_data['metric_name'] = metric_name
                    parsed_data['metric_value'] = metric_value
                    # Remove the original metric=value from the dict if it was added as a regular key-value
                    parsed_data.pop(metric_name, None)

            parsed_data['value_internal_timestamp'] = final_value_timestamp
            return parsed_data

        if 'value' in df.columns:
            print("\nParsing 'value' column based on specific InfluxDB-like format...")
            # Apply the specific parsing function for the 'value' column
            parsed_values_df = df['value'].apply(parse_value_influx_like).apply(pd.Series)

            # Merge the new parsed columns with the original DataFrame
            df = pd.concat([df, parsed_values_df], axis=1)

            print("Extracted fields from 'value' column. New columns added:")
            # Only show head if there are columns extracted
            if not parsed_values_df.empty:
                print(parsed_values_df.columns.tolist())
                # Select a few key columns to display the parsing results
                display_cols = ['value', 'measurement', 'datatype', 'machine_name', 'metric_name', 'metric_value', 'value_internal_timestamp']
                # Filter to only show columns that actually exist in the DataFrame
                existing_display_cols = [col for col in display_cols if col in df.columns]
                print(df[existing_display_cols].head().to_markdown(index=False, numalign="left", stralign="left"))
            else:
                print("No specific fields could be extracted from the 'value' column with the current parser.")

        # --- Example Manipulations (Uncomment and modify as needed) ---

        # 5. Filter data for a specific topic
        # print("\nFiltering for 'ice_data_energy_meters_abb_robot_data' topic...")
        # if 'topic' in df.columns:
        #     specific_topic_df = df[df['topic'] == 'ice_data_energy_meters_abb_robot_data']
        #     print(f"Filtered DataFrame shape: {specific_topic_df.shape}")
        #     if 'timestamp_readable' in specific_topic_df.columns and 'metric_name' in specific_topic_df.columns:
        #         print(specific_topic_df[['timestamp_readable', 'topic', 'metric_name', 'metric_value']].head().to_markdown(index=False, numalign="left", stralign="left"))


        # 6. Aggregate data - Example: Average metric value per machine and metric name
        # if 'machine_name' in df.columns and 'metric_name' in df.columns and 'metric_value' in df.columns:
        #     print("\nCalculating average metric value per machine_name and metric_name...")
        #     # Ensure metric_value is numeric for aggregation
        #     df['metric_value_numeric'] = pd.to_numeric(df['metric_value'], errors='coerce')
        #     avg_metrics = df.groupby(['machine_name', 'metric_name'])['metric_value_numeric'].mean().reset_index()
        #     print(avg_metrics.head().to_markdown(index=False, numalign="left", stralign="left"))


        # 7. Time-based filtering (using the new 'timestamp_readable' column)
        # if 'timestamp_readable' in df.columns:
        #     start_time = pd.Timestamp('2025-06-23 00:00:00')
        #     end_time = pd.Timestamp('2025-06-24 23:59:59')
        #     print(f"\nFiltering data between {start_time} and {end_time}...")
        #     time_filtered_df = df[(df['timestamp_readable'] >= start_time) & (df['timestamp_readable'] <= end_time)]
        #     print(f"Time filtered DataFrame shape: {time_filtered_df.shape}")
        #     if 'metric_name' in time_filtered_df.columns:
        #         print(time_filtered_df[['timestamp_readable', 'topic', 'metric_name', 'metric_value']].head().to_markdown(index=False, numalign="left", stralign="left"))


        # 8. Count unique topics or metric names
        # if 'topic' in df.columns:
        #     print("\nUnique topics:")
        #     print(df['topic'].unique())
        # if 'metric_name' in df.columns:
        #     print("\nUnique metric names:")
        #     print(df['metric_name'].unique())


        print("\nManipulation complete. Final DataFrame Head:")
        print(df.head().to_markdown(index=False, numalign="left", stralign="left"))
        print(f"Final DataFrame shape: {df.shape}")

        return df

    except FileNotFoundError:
        print(f"Error: The file '{filepath}' was not found.")
        return None
    except pd.errors.EmptyDataError:
        print(f"Error: The file '{filepath}' is empty.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

# --- How to use the script ---
if __name__ == "__main__":
    # Replace with the actual path to your CSV file
    if len(sys.argv) < 2:
        print("Usage: python kafka_msgs_manipulation.py <filepath>")
        sys.exit(1)
    file_name = sys.argv[1]
    processed_df = manipulate_kafka_messages_general(file_name)

    if processed_df is not None:
        # Example: Save the manipulated DataFrame to a new CSV file
        output_filepath = f"{file_name}_manipulated.csv"
        processed_df.to_csv(output_filepath, index=False)
        print(f"\nManipulated data saved to '{output_filepath}'")