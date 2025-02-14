import json
import os

def create_customer_curated(
    customer_trusted_file: str,
    accelerometer_trusted_file: str,
    output_dir: str
):
    """
    Reads customer_trusted.json and accelerometer_trusted.json,
    performs an inner join on 'email',
    and writes out customer_curated.json.
    """

    os.makedirs(output_dir, exist_ok=True)

    # 1. Read all accelerometer emails
    accel_emails = set()
    with open(accelerometer_trusted_file, 'r') as afile:
        for line in afile:
            line = line.strip()
            if not line:
                continue
            accel_record = json.loads(line)
            # Adjust the field name if it's "user" or "email"
            user_email = accel_record.get("user") or accel_record.get("email")
            if user_email:
                accel_emails.add(user_email)

    # 2. Read customer_trusted, filter only those whose email is in accel_emails
    curated_customers = []
    with open(customer_trusted_file, 'r') as cfile:
        for line in cfile:
            line = line.strip()
            if not line:
                continue
            cust_record = json.loads(line)
            c_email = cust_record.get("email")
            if c_email in accel_emails:
                curated_customers.append(cust_record)

    # 3. Write out customer_curated.json
    output_path = os.path.join(output_dir, "customer_curated.json")
    with open(output_path, 'w') as outfile:
        for cust in curated_customers:
            outfile.write(json.dumps(cust) + "\n")

    print(f"Wrote {len(curated_customers)} curated customers to {output_path}")


if __name__ == "__main__":
    customer_trusted_path = "data/trusted/customer_trusted.json"
    accelerometer_trusted_path = "data/trusted/accelerometer_trusted.json"
    curated_output_dir = "data/curated"

    create_customer_curated(
        customer_trusted_path,
        accelerometer_trusted_path,
        curated_output_dir
    )
