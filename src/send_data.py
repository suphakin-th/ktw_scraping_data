import csv
import json
import requests

def detect_delimiter(file_path):
	with open(file_path, mode='r', encoding='utf-8') as file:
		first_line = file.readline()
		if ',' in first_line:
			return ','
		elif '\t' in first_line:
			return '\t'
		return ','

def read_csv(file_path):
    delimiter = detect_delimiter(file_path)
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=delimiter)
        headers = [h.strip() for h in next(reader)]  # Strip spaces from headers
        
        all_rows = [dict(zip(headers, row)) for row in reader]
        return headers, all_rows, delimiter

def send_bulk_update(data_chunk):
	print(f"Sending bulk update for {len(data_chunk)} products")
	url = "https://www.exogro.co.th/wp-json/v2/product/api"
	headers = {
		"Content-Type": "application/json",
		"Authorization": "Basic c2hvcF9tYW5hZ2VyMTooTFBySW0mKWdJcTk1N1VoMUQxdTF3d28=",
		"Cookie": "PHPSESSID=deteolss5gt31sb4qhl2l6pciu"
	}
	payload = {"bulk": data_chunk}
	response = requests.post(url, headers=headers, data=json.dumps(payload))
	print(f"Response: {response.status_code}")
	return response.status_code

def process_csv(file_path):
    headers, all_rows, delimiter = read_csv(file_path)

    # Filter rows with "pending" result
    pending_rows = [
        {
            "sku": row["sku"],
            "regular_price": row["regular_price"],
            "sale_price": row["sale_price"],
            "stock_status": row["stock_status"],
            "stock_qty": row["stock_quantity"]
        }
        for row in all_rows if row.get("result", "").strip().lower() == "pending"
    ]

    if not pending_rows:
        print("No 'pending' rows found.")
        return

    status_code = send_bulk_update(pending_rows)
    print(f"API Response: {status_code}")

    if status_code == 200:
        # Update 'result' column in all_rows
        for row in all_rows:
            if row.get("result", "").strip().lower() == "pending":
                row["result"] = "updated"

        # Write back the updated CSV
        with open(file_path, mode='w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter=delimiter)
            writer.writerow(headers)
            for row in all_rows:
                writer.writerow([row[h] for h in headers])

        print("CSV updated successfully.")

if __name__ == "__main__":
	print("Start Send Data")
	csv_file_path = "ktw_products.csv"
	process_csv(csv_file_path)

