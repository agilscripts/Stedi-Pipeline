import json
import os

def filter_accelerometer_directory_to_trusted(
    accelerometer_dir: str,
    customer_trusted_file_path: str,
    output_dir: str
):
    """
    1. Reads all JSON files in `accelerometer_dir` (e.g. accelerometer-1.json, accelerometer-2.json, etc.)
    2. Reads local JSON lines from `customer_trusted_file_path`
    3. Filters accelerometer rows to only those whose user/email is in customer_trusted
    4. Writes one combined `accelerometer_trusted.json` in `output_dir`
    """

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # 1. Read the set of "trusted" emails from customer_trusted.json
    with open(customer_trusted_file_path, 'r') as cfile:
        trusted_lines = cfile.readlines()

    trusted_emails = set()
    for line in trusted_lines:
        line = line.strip()
        if not line:
            continue
        cust_record = json.loads(line)
        if "email" in cust_record:
            trusted_emails.add(cust_record["email"])

    # 2. Iterate over all .json files in accelerometer_dir
    filtered_accel = []
    for filename in os.listdir(accelerometer_dir):
        if filename.endswith(".json"):
            file_path = os.path.join(accelerometer_dir, filename)
            with open(file_path, 'r') as afile:
                accel_lines = afile.readlines()

            # 3. Filter only rows whose user/email is in trusted_emails
            for line in accel_lines:
                line = line.strip()
                if not line:
                    continue
                accel_record = json.loads(line)
                user_email = accel_record.get("user")
                if user_email in trusted_emails:
                    filtered_accel.append(accel_record)

    # 4. Write out the combined filtered data
    output_file_path = os.path.join(output_dir, "accelerometer_trusted.json")
    with open(output_file_path, 'w') as outfile:
        for rec in filtered_accel:
            outfile.write(json.dumps(rec) + "\n")

    print(f"Found {len(filtered_accel)} matching records across all JSON files in {accelerometer_dir}")
    print(f"Wrote combined results to {output_file_path}")


if __name__ == "__main__":
    # Adjust these paths as needed for your local setup
    accelerometer_folder = "/Users/audrey/PycharmProjects/stedi-data-pipeline/starter/accelerometer/landing"
    customer_trusted_file = "/Users/audrey/PycharmProjects/stedi-data-pipeline/glue_jobs/data/trusted/customer_trusted.json"
    out_dir = "./data/trusted"

    filter_accelerometer_directory_to_trusted(
        accelerometer_dir=accelerometer_folder,
        customer_trusted_file_path=customer_trusted_file,
        output_dir=out_dir
    )
