import csv
import json
import os
import requests
import logging
from itertools import islice

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

def read_csv_header(file_path):
    """Read only the header row from CSV file"""
    file_path = os.path.abspath(file_path)
    delimiter = detect_delimiter(file_path)
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=delimiter)
        headers = [h.strip() for h in next(reader)]  # Strip spaces from headers
        return headers, delimiter

def read_csv_in_chunks(file_path, chunk_size=100):
    """Generator to read CSV file in chunks"""
    file_path = os.path.abspath(file_path)
    delimiter = detect_delimiter(file_path)
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=delimiter)
        headers = [h.strip() for h in next(reader)]  # Strip spaces from headers
        
        while True:
            # Read chunk_size rows at a time
            chunk = list(islice(reader, chunk_size))
            if not chunk:
                break
            
            # Convert rows to dictionaries
            rows = [dict(zip(headers, [row[i] if i < len(row) else '' for i in range(len(headers))])) for row in chunk]
            yield headers, rows, delimiter

def apply_discount(price, brand, config):
    """
    Apply discount to a price based on brand and configuration
    
    Args:
        price (str): The price string, which may contain currency symbols or formatting
        brand (str): The brand name
        config (dict): Configuration with discount ratios
        
    Returns:
        tuple: (discounted_price as str, discount_ratio used)
    """
    # logging.info(f"Applying discount for brand '{brand}'")
    
    if not price or not isinstance(price, str):
        # logging.warning(f"Invalid price value: {price}")
        return "0.0", 1.0
    
    try:
        # Clean price string - remove currency symbols, commas, and spaces
        cleaned_price = price.replace('฿', '').replace('THB', '').replace(',', '').strip()
        
        # Convert to float
        price_float = float(cleaned_price)
        # logging.info(f"Original price: {price_float}")
        
        # Normalize brand name for lookup - extract the first part before any slash
        normalized_brand = brand.lower().strip() if brand else "unknown"
        # Handle brand names with slashes (like "EBARA/เอบาร่า" should match "ebara")
        if '/' in normalized_brand:
            normalized_brand = normalized_brand.split('/')[0].strip()
        
        # logging.info(f"Normalized brand: {normalized_brand}")
        
        # Use functional approach with filter/lambda to find partial matches
        matching_brands = list(filter(lambda brand_key: brand_key in normalized_brand, config["SP_BRAND_DC_RATIO"].keys()))
        # Use first match if any found, otherwise default to OTHER_BRAND_DC_RATIO
        discount_ratio = config["SP_BRAND_DC_RATIO"][matching_brands[0]] if matching_brands else config["OTHER_BRAND_DC_RATIO"]
        # if matching_brands:
        #     logging.info(f"Found partial match: '{matching_brands[0]}' in '{normalized_brand}'")
        
        # Apply discount and round to 2 decimal places
        discounted_price = round(price_float * discount_ratio, 2)
        # logging.info(f"Discounted price: {discounted_price}")
        
        return str(discounted_price), discount_ratio
        
    except (ValueError, TypeError) as e:
        logging.error(f"Error applying discount to price '{price}': {str(e)}")
        # Return original price if conversion fails
        return str(price), 1.0
    except Exception as e:
        logging.error(f"Unexpected error in apply_discount: {str(e)}")
        return str(price), 1.0

def send_bulk_update(data_chunk):
    logging.info(f"Sending bulk update for {len(data_chunk)} products")
    url = "https://www.exogro.co.th/wp-json/v2/product/api"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic c2hvcF9tYW5hZ2VyMTooTFBySW0mKWdJcTk1N1VoMUQxdTF3d28="
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
            writer.writerow([item.get(col, '') for col in columns])
    logging.info(f"Created {file_name} with {len(items)} records")

def append_to_result_csv(items, file_name, columns, file_exists=False):
    mode = 'a' if file_exists else 'w'
    with open(file_name, mode=mode, encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(columns)
        for item in items:
            writer.writerow([item.get(col, '') for col in columns])
    logging.info(f"Appended {len(items)} records to {file_name}")

def process_csv(file_path: str, cfg: dict, xconfig: dict, chunk_size=5000):
    """Process CSV file in chunks"""
    # Get CSV headers first
    headers, delimiter = read_csv_header(file_path)
    
    # Initialize result containers
    all_updated_items = []
    all_not_found_items = []
    chunk_counter = 0
    
    # Initial message
    send_telegram_message("Starting to process CSV in chunks", cfg['chat_id'], cfg['telegram_token'])
    # Create/reset result CSV files
    open("updated.csv", 'w').close()
    open("not_found.csv", 'w').close()
    updated_file_exists = False
    not_found_file_exists = False
    
    # Process the CSV in chunks
    for headers, chunk_rows, delimiter in read_csv_in_chunks(file_path, chunk_size):
        chunk_counter += 1
        logging.info(f"Processing chunk {chunk_counter} with {len(chunk_rows)} rows")
        send_telegram_message(f"Processing chunk {chunk_counter} with {len(chunk_rows)} rows", cfg['chat_id'], cfg['telegram_token'])
        
        if not chunk_rows or len(chunk_rows) < 1:
            logging.info("Empty chunk, skipping")
            continue
        
        # Process this chunk
        pending_rows = []
        sku_to_row_map = {}
        
        for row in chunk_rows:
            brand = row.get("brand", "").strip()
            final_price, discount_ratio = apply_discount(row.get("sale_price", ""), brand, xconfig)
            pending_rows.append({
                "sku": row["sku"],
                "brand": brand,
                "regular_price": row["regular_price"],
                "sale_price": final_price,
                "stock_status": 1 if int(row["stock_quantity"]) > 0 else 0,
                "stock_qty": row["stock_quantity"]
            })
            
            # Store reference to original row by SKU along with the discount information
            row["discount_ratio"] = discount_ratio
            row["final_price"] = final_price
            sku_to_row_map[row["sku"]] = row
        
        if not pending_rows:
            logging.info("No rows to process in this chunk, continuing")
            continue
        
        # Send this chunk to API
        status_code, response_text = send_bulk_update(pending_rows)
        logging.info(f"API Response for chunk {chunk_counter}: {status_code}")
        
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
                            
                            # Create result item
                            common_item = {
                                "sku": sku,
                                "brand": original_row.get("brand", ""),
                                "stock_qty": original_row.get("stock_quantity", ""),
                                "stock_status": 1 if int(original_row.get("stock_quantity", 0)) > 0 else 0,
                                "sale_price": original_row.get("sale_price", ""),
                                "regular_price": original_row.get("regular_price", ""),
                                "post_id": post_id,
                                "discount_ratio": original_row.get("discount_ratio", 1.0),
                                "final_price": original_row.get("final_price", "")
                            }
                            
                            if message == "updated" and status == "success":
                                updated_items.append(common_item)
                                all_updated_items.append(common_item)
                            elif message == "not_found" or status == "error":
                                not_found_items.append(common_item)
                                all_not_found_items.append(common_item)
                
                # Append to result CSV files
                if updated_items:
                    append_to_result_csv(
                        updated_items, 
                        "updated.csv", 
                        ["sku", "brand", "stock_qty", "stock_status", "sale_price", "regular_price", "post_id", "discount_ratio", "final_price"],
                        file_exists=updated_file_exists
                    )
                    updated_file_exists = True
                
                if not_found_items:
                    append_to_result_csv(
                        not_found_items, 
                        "not_found.csv", 
                        ["sku", "brand", "stock_qty", "stock_status", "sale_price", "regular_price"],
                        file_exists=not_found_file_exists
                    )
                    not_found_file_exists = True
                
                send_telegram_message(f"Successfully processed chunk {chunk_counter}", cfg['chat_id'], cfg['telegram_token'])
                
            except json.JSONDecodeError:
                send_telegram_message(f"Failed to parse API response as JSON in chunk {chunk_counter}", cfg['chat_id'], cfg['telegram_token'])
                logging.error(f"Failed to parse API response as JSON in chunk {chunk_counter}")
            except Exception as e:
                send_telegram_message(f"Error processing API response in chunk {chunk_counter}: {str(e)}", cfg['chat_id'], cfg['telegram_token'])
                logging.error(f"Error processing API response in chunk {chunk_counter}: {str(e)}")
        else:
            send_telegram_message(f"API call failed with status code: {status_code} in chunk {chunk_counter}", cfg['chat_id'], cfg['telegram_token'])
            logging.error(f"API call failed with status code: {status_code} in chunk {chunk_counter}")
    
    # Final summary
    summary_message = f"Processing complete. Total: {chunk_counter} chunks, {len(all_updated_items)} updated, {len(all_not_found_items)} not found"
    send_telegram_message(summary_message, cfg['chat_id'], cfg['telegram_token'])
    logging.info(summary_message)

if __name__ == "__main__":
    logging.info("Start Send Data to exogro with chunking")
    # Read token from config file
    x_cfg_path = os.path.join(os.getcwd(), "src", "xconfig.json")
    xconfig = read_config(x_cfg_path)
    cfg_path = os.path.join(os.getcwd(), "src", "config.json")
    cfg = read_config(cfg_path)
    send_telegram_message("Start Send Data to exogro (Python) with chunking", cfg['chat_id'], cfg['telegram_token'])
    
    # Set chunk size - adjust this based on your system capabilities and file size
    CHUNK_SIZE = 30000  # Process 100 rows at a time
    
    # Process CSV in chunks
    ktw_csv_path = os.path.join(os.getcwd(), "ktw_products.csv")
    process_csv(ktw_csv_path, cfg, xconfig, chunk_size=CHUNK_SIZE)