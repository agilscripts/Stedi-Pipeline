import json
import os
import glob

def filter_step_trainer_landing_to_trusted(step_trainer_folder_path, customer_curated_file_path, output_dir):
    """
    1. Reads multiple JSON files for step trainer landing data
    2. Reads local JSON lines for customer_curated data
    3. Filters step trainer records to only those whose serialNumber is in the curated customer data
    4. Writes the filtered records to step_trainer_trusted.json
    """

    os.makedirs(output_dir, exist_ok=True)

    # Build a set of valid serial numbers from customer_curated
    valid_serials = set()
    with open(customer_curated_file_path, 'r') as cfile:
        for line in cfile:
            line = line.strip()
            if not line:
                continue
            cust_record = json.loads(line)
            if "serialNumber" in cust_record:
                valid_serials.add(cust_record["serialNumber"])


    trusted_records = []

    # Read all JSON files in the step_trainer_folder_path
    json_file_pattern = os.path.join(step_trainer_folder_path, '*.json')
    for json_file in glob.glob(json_file_pattern):
        with open(json_file, 'r') as stfile:
            for line in stfile:
                line = line.strip()
                if not line:
                    continue
                step_record = json.loads(line)
                if step_record.get("serialNumber") in valid_serials:
                    trusted_records.append(step_record)

    # Write out the filtered step trainer data
    output_file_path = os.path.join(output_dir, "step_trainer_trusted.json")
    with open(output_file_path, 'w') as outfile:
        for rec in trusted_records:
            outfile.write(json.dumps(rec) + "\n")

    print(f"Wrote {len(trusted_records)} filtered step trainer records to {output_file_path}")


if __name__ == "__main__":
    step_trainer_folder = "/Users/audrey/PycharmProjects/stedi-data-pipeline/starter/step_trainer/landing"
    customer_curated_file = "/s3/curated/customer_curated.json"
    output_dir = "/s3/trusted"

    filter_step_trainer_landing_to_trusted(step_trainer_folder, customer_curated_file, output_dir)
