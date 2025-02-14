import json
import os
import glob

def filter_step_trainer_landing_to_trusted(
    step_trainer_landing_dir: str,
    customer_trusted_file_path: str,
    output_dir: str
):
    """
    1. Reads multiple Step Trainer landing JSON files from step_trainer_landing_dir.
    2. Reads local JSON lines for customer_trusted data to get a set of valid serial numbers.
    3. Filters Step Trainer records to only those whose serialNumber is in the trusted customer list.
    4. Writes a single aggregated step_trainer_trusted.json to output_dir.
    """

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Read customer_trusted to get valid serial numbers
    trusted_serial_numbers = set()
    with open(customer_trusted_file_path, 'r') as cfile:
        for line in cfile:
            line = line.strip()
            if not line:
                continue
            cust_record = json.loads(line)
            if "serialNumber" in cust_record:
                trusted_serial_numbers.add(cust_record["serialNumber"])

    # Collect all step trainer records from the landing folder
    all_step_records = []
    # For example, if files are named step_trainer-1.json, step_trainer-2.json, etc.
    pattern = os.path.join(step_trainer_landing_dir, "step_trainer-*.json")
    step_files = glob.glob(pattern)

    for sfile_path in step_files:
        with open(sfile_path, 'r') as sfile:
            lines = sfile.readlines()

        for line in lines:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            all_step_records.append(record)

    # Filter the Step Trainer records to only include those with a valid serial number
    filtered_step_records = [
        rec for rec in all_step_records
        if rec.get("serialNumber") in trusted_serial_numbers
    ]

    # Write out the filtered Step Trainer records
    output_file_path = os.path.join(output_dir, "step_trainer_trusted.json")
    with open(output_file_path, 'w') as outfile:
        for rec in filtered_step_records:
            outfile.write(json.dumps(rec) + "\n")

    print(f"Wrote {len(filtered_step_records)} filtered Step Trainer records to {output_file_path}")


if __name__ == "__main__":
    # Adjust these paths for your local setup

    # Folder containing step_trainer-1.json, step_trainer-2.json, etc.
    step_trainer_landing_dir = (
        "/Users/audrey/PycharmProjects/"
        "stedi-data-pipeline/starter/step_trainer/landing"
    )

    # The path to your trusted customers JSON file
    customer_trusted_file = (
        "/Users/audrey/PycharmProjects/"
        "stedi-data-pipeline/glue_jobs/data/trusted/customer_trusted.json"
    )

    # Where to put the final step_trainer_trusted.json
    output_directory = (
        "/Users/audrey/PycharmProjects/"
        "stedi-data-pipeline/glue_jobs/data/trusted"
    )

    filter_step_trainer_landing_to_trusted(
        step_trainer_landing_dir,
        customer_trusted_file,
        output_directory
    )
