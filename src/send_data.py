import csv
import json
import os
import requests
import logging

logging.basicConfig(level=logging.INFO)

def detect_delimiter(file_path):
    file_path = os.path.abspath(file_path)
    with open(file_path, mode='r', encoding='utf-8') as file:
        first_line = file.readline()
        if ',' in first_line:
            return ','
        elif '\t' in first_line:
            return '\t'
        return ','
    
def send_telegram_message(message, chat_id, token):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message
    }
    response = requests.post(url, data=payload)
    return response.status_code, response.text

def read_config(file_path):
    file_path = os.path.abspath(file_path)
    with open(file_path, mode='r', encoding='utf-8', newline='') as file:
        return json.load(file)

def read_csv(file_path):
    file_path = os.path.abspath(file_path)
    delimiter = detect_delimiter(file_path)
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=delimiter)
        headers = [h.strip() for h in next(reader)]  # Strip spaces from headers
        
        all_rows = [dict(zip(headers, [row[i] if i < len(row) else '' for i in range(len(headers))])) for row in reader]
        logging.info(f"Read {len(all_rows)} rows from CSV")
        return headers, all_rows, delimiter

def apply_discount(price, brand, config):
    try:
        price = float(price) if price else 0.0
    except ValueError:
        return price  # Return as is if it's not a valid number
    
    brand = brand.lower().strip()
    discount_ratio = config["SP_BRAND_DC_RATIO"].get(brand, config["OTHER_BRAND_DC_RATIO"])
    return round(price * discount_ratio, 2)  # Apply discount and round to 2 decimals

def send_bulk_update(data_chunk):
    logging.info(f"Sending bulk update for {len(data_chunk)} products")
    logging.info("First 5 products in bulk update:")
    for product in data_chunk[:5]:
        logging.info(f"SKU: {product.get('sku', 'N/A')}")

    url = "https://www.exogro.co.th/wp-json/v2/product/api"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic c2hvcF9tYW5hZ2VyMTooTFBySW0mKWdJcTk1N1VoMUQxdTF3d28=",
        "Cookie": "PHPSESSID=deteolss5gt31sb4qhl2l6pciu"
    }
    payload = {"bulk": data_chunk}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    logging.info(f"Total products in payload: {len(payload['bulk'])}")
    
    return response.status_code, response.text

def create_result_csv(items, file_name, columns):
    with open(file_name, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        for item in items:
            writer.writerow([item[col] for col in columns])
    logging.info(f"Created {file_name} with {len(items)} records")

def process_csv(file_path: str, cfg: json, xconfig: json):
    # Read CSV
    send_telegram_message("Read Data from CSV", cfg['chat_id'], cfg['telegram_token'])
    headers, all_rows, delimiter = read_csv(file_path)
    pending_rows = []
    
    # Create a mapping of SKU to original row for easy access later
    sku_to_row_map = {}
    if not all_rows or len(all_rows) < 1:
        logging.info(f"API Response: {status_code}")
        send_telegram_message("Send update data fail can not read csv.", cfg['chat_id'], cfg['telegram_token'])
        return
    for row in all_rows:
        brand = row.get("brand", "").strip()
        sale_price = apply_discount(row.get("sale_price", ""), brand, xconfig)
        pending_rows.append({
            "sku": row["sku"],
            "brand": brand,
            "regular_price": row["regular_price"],
            "sale_price": str(sale_price),
            "stock_status": row["stock_status"],
            "stock_qty": row["stock_quantity"]
        })
        
        # Store reference to original row by SKU
        sku_to_row_map[row["sku"]] = row
    if not pending_rows or len(pending_rows) < 1:
        send_telegram_message("No rows found to process.", cfg['chat_id'], cfg['telegram_token'])
        logging.info("No rows found to process.")
        return

    status_code, response_text = send_bulk_update(pending_rows)
    logging.info(f"API Response: {status_code}")
    send_telegram_message("Send update data to EXOGRO WITH STATUS {}.".format(status_code), cfg['chat_id'], cfg['telegram_token'])

    if status_code == 200:
        try:
            response_data = json.loads(response_text)
            updated_items = []
            not_found_items = []
            
            # Process each result item
            if "result" in response_data and isinstance(response_data["result"], list):
                for result_item in response_data["result"]:
                    sku = result_item.get("sku")
                    status = result_item.get("status")
                    message = result_item.get("message")
                    post_id = result_item.get("post_id")  # Get post_id from response
                    
                    # Get original row data for this SKU
                    if sku in sku_to_row_map:
                        original_row = sku_to_row_map[sku]
                        
                        # Update result field in the original CSV data
                        original_row["result"] = message
                        
                        # Add to appropriate result list
                        common_item = {
                            "sku": sku,
                            "brand": original_row.get("brand", ""),
                            "stock_qty": original_row.get("stock_quantity", ""),  # Use stock_quantity from original CSV
                            "sale_price": original_row.get("sale_price", ""),
                            "regular_price": original_row.get("regular_price", ""),
                            "post_id": post_id  # Add post_id from API response
                        }
                        
                        if message == "updated" and status == "success":
                            updated_items.append(common_item)
                        elif message == "not_found" or status == "error":
                            not_found_items.append(common_item)
            
            # Write updated data back to CSV
            with open(file_path, mode='w', encoding='utf-8', newline='') as file:
                writer = csv.writer(file, delimiter=delimiter)
                writer.writerow(headers)
                for row in all_rows:
                    writer.writerow([row[h] for h in headers])
            
            # Create result CSV files
            create_result_csv(updated_items, "updated.csv", 
                ["sku", "brand", "stock_qty", "sale_price", "regular_price", "post_id"])
            create_result_csv(not_found_items, "not_found.csv", 
                ["sku", "brand", "stock_qty", "sale_price", "regular_price", "post_id"])
            send_telegram_message("CSV updated successfully.", cfg['chat_id'], cfg['telegram_token'])
            logging.info("CSV updated successfully.")
            
        except json.JSONDecodeError:
            send_telegram_message("CSV update fail : Failed to parse API response as JSON.", cfg['chat_id'], cfg['telegram_token'])
            logging.info("Failed to parse API response as JSON")
        except Exception as e:
            send_telegram_message(f"CSV update fail : Error processing API response: {str(e)}.", cfg['chat_id'], cfg['telegram_token'])
            logging.info(f"Error processing API response: {str(e)}")
    else:
        send_telegram_message(f"API call failed with status code: {status_code}", cfg['chat_id'], cfg['telegram_token'])
        logging.info(f"API call failed with status code: {status_code}")


if __name__ == "__main__":
    logging.info("Start Send Data to exogro")
    # Read token from config file
    x_cfg_path = os.path.join(os.getcwd(), "src", "xconfig.json")
    xconfig = read_config(x_cfg_path)
    cfg_path = os.path.join(os.getcwd(), "src", "config.json")
    cfg = read_config(cfg_path)
    send_telegram_message("Start Send Data to exogro (Python)", cfg['chat_id'], cfg['telegram_token'])
    # Send notication to Telegram
    ktw_csv_path = os.path.join(os.getcwd(), "ktw_products.csv")
    process_csv(ktw_csv_path, cfg, xconfig)
