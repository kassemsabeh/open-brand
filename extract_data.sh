AMAZON_META_DATA="./All_Amazon_Meta.json"

python3 extract_data.py \
--input_amazon_product_metadata_json_filename="${AMAZON_META_DATA}" \
--input_tags_filename='./labels/base_labels.jsonl' \
--output_dir_name='./datasets' \
--output_json_filename='./datasets/az_base_dataset.jsonl'

python3 extract_data.py \
--input_amazon_product_metadata_json_filename="${AMAZON_META_DATA}" \
--input_tags_filename='./labels/new_cat_labels.jsonl' \
--output_dir_name='./datasets' \
--output_json_filename='./datasets/az_new_cat_dataset.jsonl'
