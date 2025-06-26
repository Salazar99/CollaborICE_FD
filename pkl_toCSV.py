# pkl_to_csv.py

import pickle
import pandas as pd
import argparse
import os
import sys

def convert_pkl_to_csv(input_path, output_path):
    """
    Loads data from a .pkl file, converts it to a pandas DataFrame,
    and saves it as a .csv file.

    Args:
        input_path (str): The file path for the input .pkl file.
        output_path (str): The file path for the output .csv file.
    """
    try:
        # --- Step 1: Load the pickle file ---
        print(f"Loading pickle file from: {input_path}")
        with open(input_path, 'rb') as pkl_file:
            # 'rb' is important for reading pickle files
            data = pickle.load(pkl_file)
        print(f"Successfully loaded data. Detected type: {type(data)}")

        # --- Step 2: Convert the data to a pandas DataFrame ---
        # This handles multiple common data structures automatically.
        if isinstance(data, pd.DataFrame):
            df = data
            print("Data is already a pandas DataFrame.")
        else:
            # Attempt to convert other data types (e.g., list of dicts)
            try:
                df = pd.DataFrame(data)
                print("Successfully converted data to a pandas DataFrame.")
            except Exception as e:
                print(f"Error: Could not convert the data into a DataFrame.", file=sys.stderr)
                print(f"The data in the pickle file is of type '{type(data)}', which is not directly convertible.", file=sys.stderr)
                print(f"Please ensure the .pkl file contains a DataFrame, a list of dictionaries, or another table-like structure.", file=sys.stderr)
                sys.exit(1) # Exit with an error code

        # --- Step 3: Save the DataFrame to a CSV file ---
        print(f"Saving DataFrame to CSV at: {output_path}")
        # We use index=False to avoid writing the DataFrame's index as a column
        df.to_csv(output_path, index=False, encoding='utf-8')
        print("Conversion successful!")

    except FileNotFoundError:
        print(f"Error: The file '{input_path}' was not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    # --- Set up Argument Parser for command-line use ---
    parser = argparse.ArgumentParser(
        description="Convert a .pkl file into a .csv file.",
        epilog="Example: python pkl_to_csv.py my_data.pkl -o my_data.csv"
    )

    parser.add_argument(
        "input_file",
        help="The path to the input .pkl file."
    )

    parser.add_argument(
        "-o", "--output_file",
        help="The path for the output .csv file. (Optional: If not provided, it will use the input name with a .csv extension)"
    )

    args = parser.parse_args()

    # --- Determine the output file path ---
    input_path = args.input_file
    output_path = args.output_file

    if not output_path:
        # If no output path is given, create one from the input path
        # e.g., 'data/my_file.pkl' becomes 'data/my_file.csv'
        base_name = os.path.splitext(input_path)[0]
        output_path = f"{base_name}.csv"
        
        # Prevent accidentally overwriting the input if it has no extension
        if output_path == input_path:
            output_path += ".csv"

    # --- Run the conversion function ---
    convert_pkl_to_csv(input_path, output_path)