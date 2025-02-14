import json
import os

def filter_customers_landing_to_trusted(landing_file_path: str, output_dir: str):
    """
    1. Reads a local JSON file of customer landing data
    2. Filters out customers who do not have shareWithResearchAsOfDate
    3. Writes filtered JSON lines to an output directory
    """

    # Make sure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    with open(landing_file_path, 'r') as infile:
        lines = infile.readlines()

    # We'll store only the records with a valid shareWithResearchAsOfDate
    trusted_records = []
    for line in lines:
        line = line.strip()
        if not line:
            continue  # skip any blank lines
        record = json.loads(line)

        # If "shareWithResearchAsOfDate" exists and is not blank/None
        if record.get("shareWithResearchAsOfDate"):
            trusted_records.append(record)

    # Write out the filtered data
    output_file_path = os.path.join(output_dir, "customer_trusted.json")
    with open(output_file_path, 'w') as outfile:
        for rec in trusted_records:
            outfile.write(json.dumps(rec) + "\n")

    print(f"Wrote {len(trusted_records)} filtered records to {output_file_path}")


if __name__ == "__main__":
    # Change this path to wherever your actual JSON file is stored
    # For example, if your file is in 'starter/customer/landing/customer_landing.json', do:
    landing_file = "/Users/audrey/PycharmProjects/stedi-data-pipeline/starter/customer/landing/customer_landing.json"

    # This is the directory where the filtered JSON will be written
    out_dir = "./data/trusted"

    filter_customers_landing_to_trusted(landing_file, out_dir)
