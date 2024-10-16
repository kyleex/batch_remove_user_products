import os
import dotenv
import requests
from requests.auth import HTTPBasicAuth
import csv
from datetime import datetime, timedelta
from collections import Counter

# Charger le fichier .env pour accéder aux variables d'environnement
dotenv.load_dotenv()

# Configuration
ATLASSIAN_DOMAIN = os.getenv('ATLASSIAN_DOMAIN')
ATLASSIAN_ACCOUNT_EMAIL = os.getenv('ATLASSIAN_ACCOUNT_EMAIL')
ATLASSIAN_ACCOUNT_API_TOKEN = os.getenv('ATLASSIAN_ACCOUNT_API_TOKEN')

NB_days_since_last_connection = 90  # Variable pour le nombre de jours depuis la dernière connexion
NB_days_since_account_added_in_org = 30  # Variable pour le nombre de jours depuis que le compte a été ajouté dans l'org studi-pedago
groups_giving_access_to_product = [
    {
        "product_name": "Jira Service Management",
        "group_id": "",
        "group_name": "jira-servicedesk-users"
    },
    {
        "product_name": "Jira",
        "group_id": "",
        "group_name": "jira-software-users"
    },
    {
        "product_name": "Confluence",
        "group_id": "cce129aa-814b-45c7-8136-d9aaebc1b3dd",
        "group_name": ""
    }
]

def convert_date(date_str):
    """Convertit une date au format 'd M Y' en 'YYYY-MM-DD'."""
    try:
        return datetime.strptime(date_str, '%d %b %Y').strftime('%Y-%m-%d')
    except ValueError:
        return None if date_str == 'Never accessed' else date_str

def remove_user_from_group(account_id, group_id):
    """Supprime un utilisateur d'un groupe Atlassian."""
    url = f'https://{ATLASSIAN_DOMAIN}/rest/api/3/group/user'
    params = {
        'groupId': group_id,
        'accountId': account_id
    }
    auth = HTTPBasicAuth(ATLASSIAN_ACCOUNT_EMAIL, ATLASSIAN_ACCOUNT_API_TOKEN)
    response = requests.delete(url, params=params, auth=auth)
    return response

def process_users(file_path):
    """Traite les utilisateurs à partir d'un fichier CSV et retourne ceux à désactiver."""
    users_to_deactivate = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            for key in row.keys():
                row[key] = convert_date(row[key])

            if row['User status'] == 'Active':
                added_date = datetime.strptime(row['Added to org'], '%Y-%m-%d')
                if added_date < datetime.now() - timedelta(days=NB_days_since_account_added_in_org):
                    for product in ['Jira Service Management - <Your DOMAIN NAME>', 
                                    'Jira - <Your DOMAIN NAME>', 
                                    'Confluence - <Your DOMAIN NAME>']:
                        product_status = row[product]
                        last_seen_key = 'Last seen in ' + product
                        last_seen_date = row[last_seen_key]

                        if last_seen_date and datetime.strptime(last_seen_date, '%Y-%m-%d') < datetime.now() - timedelta(days=NB_days_since_last_connection):
                            product_name = product.replace(' - <Your DOMAIN NAME>', '')
                            group_info = next((g for g in groups_giving_access_to_product if g['product_name'] == product_name), None)
                            if group_info:
                                if product_name == "Jira Service Management":
                                    # Désactiver si le statut contient "User"
                                    if "User" in product_status:
                                        users_to_deactivate.append({
                                            'User id': row['User id'],
                                            'product_name': product_name,
                                            'group_id': group_info['group_id'],
                                            'group_name': group_info['group_name'],
                                            'last_seen_date': last_seen_date
                                        })
                                else:
                                    # Désactiver si le statut est exactement "User"
                                    if product_status == 'User':
                                        users_to_deactivate.append({
                                            'User id': row['User id'],
                                            'product_name': product_name,
                                            'group_id': group_info['group_id'],
                                            'group_name': group_info['group_name'],
                                            'last_seen_date': last_seen_date
                                        })
    return users_to_deactivate

def log_user_removal(user, response, log_file):
    """Enregistre le résultat de la tentative de suppression d'un utilisateur dans un fichier log."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    endpoint = response.url
    action = 'User Removal'
    status_code = response.status_code

    if status_code == 200:
        log_file.write(f"{timestamp} - Endpoint: {endpoint} - Action: {action} - User: {user['User id']} removed from Group: {user['group_id']} - Status: Done\n")
    else:
        log_file.write(f"{timestamp} - Endpoint: {endpoint} - Action: {action} - User: {user['User id']} from Group: {user['group_id']} - Status: Failed - Error Code: {status_code} - Reason: {response.reason}\n")

def main():

    # Atlassian users export with products access file path
    file_path_all_atlassian_users = 'export-users.csv'

    # Current date and time
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Output file path of Unsused user products
    output_file_path = f'exports/remove_user_products/user_products_unused_to_remove_{timestamp}.csv'
    log_file_path = f'exports/remove_user_products/user_products_unused_to_remove_{timestamp}.log'

    # Process to identify the list of user to desactivate product
    users_to_deactivate = process_users(file_path_all_atlassian_users)

    # Count occurences for each user_id in the created csv file
    user_id_counts = Counter(user['User id'] for user in users_to_deactivate)

    # export "Unsused user products" in csv
    with open(output_file_path, mode='w', newline='', encoding='utf-8') as output_file:
        # Header
        fieldnames = ['User id', 'product_name', 'group_id', 'group_name', 'last_seen_date', 'occurrence_user_id']
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        # values
        for user in users_to_deactivate:
            user['occurrence_user_id'] = user_id_counts[user['User id']]
            writer.writerow(user)

            
    # Read the csv file created to remove user from the group to desactivate product
    with open(log_file_path, mode='w', encoding='utf-8') as log_file:
        for user in users_to_deactivate:
            response = remove_user_from_group(user['User id'], user['group_id'])
            log_user_removal(user, response, log_file)

if __name__ == "__main__":
    main()