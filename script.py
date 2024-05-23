import pandas as pd
import xmlrpc.client

# Odoo connection setup
url_db = 'http://localhost:8001'
db = 'sale_order_create_record_db'
username = 'admin'
password = 'admin'

common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url_db))
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url_db))
uid = common.authenticate(db, username, password, {})

# Read Excel file
excel_file_path = '/home/hp/Downloads/MARCH 2024 B2C SALES.xlsx'
df = pd.read_excel(excel_file_path)

# Filter DataFrame by converting DEPOT to lowercase
df = df[df['DEPOT'].str.lower() == 'lubumbashi'.lower()]

# Initialize counters
total_records_created = 0
total_records_updated = 0

if not df.empty:
    for idx in range(0,42115):
        first_row = df.iloc[idx]
        print("\nFirst row data:", first_row)

        print("First row date:", first_row.Date)

        # This code is used to read data from column 12 to the second last column.
        search_columns = first_row.index[11:-1]
        print("search_columns:", search_columns)

        # Collect all product quantities and their respective columns.
        product_quantities = []
        for column in search_columns:
            value = first_row[column]
            if not pd.isna(value):
                product_quantities.append((column, value))

        print("Product quantities:", product_quantities)

        for product_column, product_qty in product_quantities:
            print(f"Processing product {product_column} with quantity {product_qty}")

            # This is used to search the product in product.template model
            product_ids = models.execute_kw(
                db, uid, password, 'product.product', 'search',
                [[('name', '=', product_column)]], {'limit': 1}
            )
            print("product_ids:", product_ids)

            # This one is used to fetch the data of that product if it is found
            if product_ids:
                product_data = models.execute_kw(
                    db, uid, password, 'product.product', 'read',
                    [product_ids, ['name', 'default_code', 'list_price', 'taxes_id']]
                )
                print("\n\n\n product_data", product_data)
                product_name = product_data[0]['name']
                product_code = product_data[0]['default_code']
                product_price = product_data[0]['list_price']
                product_tax = product_data[0]['taxes_id']

                print(f"Product Name: {product_name}, Product Code: {product_code}")
            else:
                print(f"No product found with name {product_column}.")
                continue

            # This is used to search 'compte client' in res.partner model
            partner_ids = models.execute_kw(
                db, uid, password, 'res.partner', 'search',
                [[('cit_gc_shop_id', 'ilike',
                   first_row['C0mpte Client'].strip().lower())]],
                {'limit': 1}
            )
            print("Partner IDs:", partner_ids)
            print("Value of 'C0mpte Client':", first_row['C0mpte Client'].strip().lower())

            # This is used to fetch the name and user_id if the partner_ids is found
            if partner_ids:
                partner_data = models.execute_kw(
                    db, uid, password, 'res.partner', 'read',
                    [partner_ids, ['name', 'user_id']]
                )
                print("Partner data:", partner_data)
                partner_id = partner_data[0]['id']
                name = partner_data[0]['name']
                user_id = partner_data[0]['user_id'][0]

                print("partner_id:", partner_id)
                print("name:", name)
                print("second_user_id_value:", user_id)

                # This is used to check if there was already a record existing
                sale_order_id = models.execute_kw(
                    db, uid, password, 'sale.order', 'search',
                    [[('partner_id', '=', partner_id)]], {'limit': 1}
                )

                # If there is a sale order existing, then this will update it else it will create a new sale order
                if sale_order_id:
                    models.execute_kw(
                        db, uid, password, 'sale.order', 'write',
                        [sale_order_id, {
                            'user_id': user_id,
                            'date_order': first_row['Date'].strftime('%Y-%m-%d %H:%M:%S'),
                            'order_line': [(0, 0, {
                                'name': product_name,
                                'product_id': product_ids[0],
                                'product_uom_qty': float(product_qty),
                                'price_unit': product_price,
                                "tax_id": product_tax
                            })]
                        }]
                    )
                    total_records_updated += 1
                    print("Updated Sale Order ID:", sale_order_id)
                else:
                    sale_order_id = models.execute_kw(
                        db, uid, password, 'sale.order', 'create', [{
                            'partner_id': partner_id,
                            'user_id': user_id,
                            'date_order': first_row['Date'].strftime('%Y-%m-%d %H:%M:%S'),
                            'order_line': [(0, 0, {
                                'name': product_name,
                                'product_id': product_ids[0],
                                'product_uom_qty': float(product_qty),
                                'price_unit': product_price,
                                "tax_id": product_tax
                            })]
                        }]
                    )
                    total_records_created += 1
                    print("Created Sale Order ID:", sale_order_id)
            else:
                print(
                    f"No partner found with cit_gc_shop_id {first_row['C0mpte Client']}.")
else:
    print("The DataFrame is empty.")

# Print totals
print("Total records created:", total_records_created)
print("Total records updated:", total_records_updated)
