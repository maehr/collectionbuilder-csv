import os
from urllib.parse import urljoin

import pandas as pd
import requests

# Configuration
OMEKA_API_URL = os.getenv("OMEKA_API_URL")
KEY_IDENTITY = os.getenv("KEY_IDENTITY")
KEY_CREDENTIAL = os.getenv("KEY_CREDENTIAL")
ITEM_SET_ID = os.getenv("ITEM_SET_ID")


# Function to get items from a collection
def get_items_from_collection(collection_id):
    url = urljoin(OMEKA_API_URL, "items")
    all_items = []
    params = {
        "item_set_id": collection_id,
        "key_identity": KEY_IDENTITY,
        "key_credential": KEY_CREDENTIAL,
        "per_page": 100,
    }

    while True:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            break
        items = response.json()
        all_items.extend(items)
        next_url = None
        for link in response.links:
            if response.links[link]["rel"] == "next":
                next_url = response.links[link]["url"]
                break
        if not next_url:
            break
        url = next_url
    return all_items


# Function to get media for an item
def get_media(item_id):
    url = urljoin(OMEKA_API_URL, f"media?item_id={item_id}")
    params = {"key_identity": KEY_IDENTITY, "key_credential": KEY_CREDENTIAL}
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        return None
    return response.json()


# Function to download file
def download_file(url, dest_path):
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"Other error occurred: {err}")


# Helper functions to extract data
def extract_prop_value(props, prop_id):
    return next(
        (
            prop.get("@value", "")
            for prop in props
            if prop.get("property_id") == prop_id
        ),
        "",
    )


def extract_prop_uri(props, prop_id):
    return next(
        (
            prop.get("@id", "")
            if "o:label" not in prop
            else f"[{prop.get('o:label', '')}]({prop.get('@id', '')})"
            for prop in props
            if prop.get("property_id") == prop_id
        ),
        "",
    )


def extract_combined_list(props):
    values = [prop.get("@value", "") for prop in props if "@value" in prop]
    uris = [
        prop.get("@id", "")
        if "o:label" not in prop
        else f"[{prop.get('o:label', '')}]({prop.get('@id', '')})"
        for prop in props
        if "@id" in prop
    ]
    combined = values + uris
    return ";".join(combined)


def extract_item_data(item):
    image_url = item.get("thumbnail_display_urls", {}).get("large", "")
    local_image_path = ""
    if image_url:
        local_image_path = f"objects/{item['o:id']}.jpg"
        download_file(image_url, local_image_path)

    return {
        "objectid": item["o:id"],
        "parentid": "",
        "title": extract_prop_value(item.get("dcterms:title", []), 1),
        "description": extract_prop_value(item.get("dcterms:description", []), 4),
        "subject": extract_combined_list(item.get("dcterms:subject", [])),
        "era": extract_prop_value(item.get("dcterms:temporal", []), 41),
        "isPartOf": extract_combined_list(item.get("dcterms:isPartOf", [])),
        "creator": extract_combined_list(item.get("dcterms:creator", [])),
        "publisher": extract_combined_list(item.get("dcterms:publisher", [])),
        "source": extract_combined_list(item.get("dcterms:source", [])),
        "date": extract_prop_value(item.get("dcterms:date", []), 7),
        "type": extract_prop_uri(item.get("dcterms:type", []), 8),
        "format": extract_prop_value(item.get("dcterms:format", []), 9),
        "extent": extract_prop_value(item.get("dcterms:extent", []), 25),
        "language": extract_prop_value(item.get("dcterms:language", []), 12),
        "relation": extract_combined_list(item.get("dcterms:relation", [])),
        "rights": extract_prop_value(item.get("dcterms:rights", []), 15),
        "license": extract_prop_value(item.get("dcterms:license", []), 49),
        "display_template": "compound_object",
        "object_location": local_image_path,
        "image_small": local_image_path,
        "image_thumb": local_image_path,
        "image_alt_text": item.get("o:alt_text", ""),
    }


def infer_display_template(mime_type):
    if "image" in mime_type:
        return "image"
    elif "pdf" in mime_type:
        return "pdf"
    elif "geo+json" in mime_type:
        return "geodata"
    else:
        return "record"


def extract_media_data(media, item_id):
    mime_type = media.get("o:media_type", "").lower()
    display_template = infer_display_template(mime_type)

    image_url = media.get("thumbnail_display_urls", {}).get("large", "")
    local_image_path = ""
    if image_url:
        local_image_path = f"objects/{media['o:id']}.jpg"
        download_file(image_url, local_image_path)

    return {
        "objectid": media["o:id"],
        "parentid": item_id,
        "title": extract_prop_value(media.get("dcterms:title", []), 1),
        "description": extract_prop_value(media.get("dcterms:description", []), 4),
        "subject": extract_combined_list(media.get("dcterms:subject", [])),
        "era": extract_prop_value(media.get("dcterms:temporal", []), 41),
        "isPartOf": extract_combined_list(media.get("dcterms:isPartOf", [])),
        "creator": extract_combined_list(media.get("dcterms:creator", [])),
        "publisher": extract_combined_list(media.get("dcterms:publisher", [])),
        "source": extract_combined_list(media.get("dcterms:source", [])),
        "date": extract_prop_value(media.get("dcterms:date", []), 7),
        "type": extract_prop_uri(media.get("dcterms:type", []), 8),
        "format": extract_prop_value(media.get("dcterms:format", []), 9),
        "extent": extract_prop_value(media.get("dcterms:extent", []), 25),
        "language": extract_prop_value(media.get("dcterms:language", []), 12),
        "relation": extract_combined_list(media.get("dcterms:relation", [])),
        "rights": extract_prop_value(media.get("dcterms:rights", []), 15),
        "license": extract_prop_value(media.get("dcterms:license", []), 49),
        "display_template": display_template,
        "object_location": local_image_path,
        "image_small": local_image_path,
        "image_thumb": local_image_path,
        "image_alt_text": media.get("o:alt_text", ""),
    }


# Main function to download item set and generate CSV
def main():
    # Download item set
    collection_id = ITEM_SET_ID
    items_data = get_items_from_collection(collection_id)

    # Extract item data
    item_records = [extract_item_data(item) for item in items_data]

    # Extract media data for each item
    media_records = []
    for item in items_data:
        item_id = item["o:id"]
        media_data = get_media(item_id)
        if media_data:
            for media in media_data:
                media_records.append(extract_media_data(media, item_id))

    # Combine item and media records
    combined_records = item_records + media_records

    # Create DataFrame
    df = pd.DataFrame(combined_records)

    # Save to CSV
    csv_path = "_data/sgb-metadata.csv"
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(
        csv_path, index=False, quoting=1
    )  # quoting=1 for quoting all fields in the CSV

    print(f"CSV file has been saved to {csv_path}")


if __name__ == "__main__":
    main()
