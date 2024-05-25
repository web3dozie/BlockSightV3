import os, csv, glob, zipfile
import pandas as pd
from collections import Counter
import backoff


def process_zip(zip_file, path_to_folder):
    with zipfile.ZipFile(os.path.join(path_to_folder, zip_file), 'r') as zip_ref:
        # Extract all the contents into the folder
        zip_ref.extractall(path_to_folder)

        for file in zip_ref.namelist():
            if file.endswith('.csv'):
                # Extract the base name of the zip file and replace the extension with .csv
                base_name = os.path.splitext(zip_file)[0]
                new_file_name = f"{base_name}.csv"

                # Rename the extracted csv file
                os.rename(os.path.join(path_to_folder, file), os.path.join(path_to_folder, new_file_name))
                break

    # Remove the original zip file
    os.remove(os.path.join(path_to_folder, zip_file))


def handle_zips(path_to_folder):
    # List all zip files in the folder
    zip_list = [file for file in os.listdir(path_to_folder) if file.endswith('.zip')]

    for zip_file in zip_list:
        process_zip(zip_file, path_to_folder)
        print(f'Successfully processed {zip_file}')


def process_csv(csv_file):
    # Read the CSV file
    df = pd.read_csv(csv_file)

    print(df.head(2))

    # Extract the "Owner" column
    owners = df['        Owner']

    # Create a new txt file with the same name as the csv file
    txt_filename = os.path.splitext(csv_file)[0] + '.txt'
    with open(txt_filename, 'w') as txt_file:
        for owner in owners:
            txt_file.write(f"{owner}\n")

    # Delete the original csv file
    os.remove(csv_file)


def handle_csvs(path_to_folder):
    # Find all csv files in the folder
    csv_list = glob.glob(os.path.join(path_to_folder, '*.csv'))

    for csv_file in csv_list:
        process_csv(csv_file)
        print(f'Successfully processed {csv_file}')


def only_unique(txt_file):
    with open(txt_file, 'r') as file:
        lines = file.readlines()

    # Remove duplicate lines
    unique_lines = list(dict.fromkeys(lines))

    # Write back to the file
    with open(txt_file, 'w') as file:
        file.writelines(unique_lines)


def handle_txts(path_to_folder):
    # List all .txt files in the folder
    txt_list = [os.path.join(path_to_folder, f) for f in os.listdir(path_to_folder) if f.endswith('.txt')]

    for txt_file in txt_list:
        only_unique(txt_file)
        print(f'Successfully removed duplicate lines in {txt_file}')


def count_wallet_addresses(folder_path):
    wallet_counts = Counter()

    # Read each file and update counts
    for filename in os.listdir(folder_path):
        if filename.endswith(".txt"):
            file_path = os.path.join(folder_path, filename)
            try:
                with open(file_path, 'r') as file:
                    wallet_addresses = file.read().splitlines()
                    wallet_counts.update(wallet_addresses)
            except IOError as e:
                print(f"Error reading file {filename}: {e}")

    # Sort by count in descending order
    sorted_wallets = sorted(wallet_counts.items(), key=lambda x: x[1], reverse=True)
    sorted_wallets = [wallet for wallet in sorted_wallets if wallet[1] >= 4]

    # Write to CSV file
    with open('wallet_counts.csv', 'w', newline='') as csvfile:
        fieldnames = ['Wallet', 'Count']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for wallet, count in sorted_wallets:
            writer.writerow({'Wallet': wallet, 'Count': count})

    print("CSV file 'wallet_counts.csv' created with wallet counts.")


def read_csv_wallets(file_path):
    """
    Reads a CSV file and returns a list of wallet addresses.

    Args:
    file_path (str): The path to the CSV file.

    Returns:
    list: A list of wallet addresses as strings.
    """
    wallets = []
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            wallets.append(row['Wallet'])
    return wallets


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def remove_wallet_from_csv(path_to_csv, wallet):
    """
    Removes specified wallets from the CSV.

    Args:
    path_to_csv (str): The path to the CSV file.

    wallets (list): wallet to be removed


    Returns:
    Nothing. If the specified wallet isn't in the CSV, fail silently 
    """
    df = pd.read_csv(path_to_csv)
    indices_to_remove = df.index[df.iloc[:, 0] == wallet].tolist()  # should only be one entry
    df = df.drop(indices_to_remove)
    df.to_csv(path_to_csv, index=False)


def wallet_processor(folder_path: str):
    handle_zips(folder_path)
    handle_csvs(folder_path)
    handle_txts(folder_path)
    count_wallet_addresses(folder_path)
