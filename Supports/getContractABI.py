import requests
import os
import json

def get_contract_abi(contract_address: str, api_key: str) -> dict:
    """
    Fetch the ABI of a smart contract from Etherscan.

    :param contract_address: The address of the smart contract.
    :param api_key: Your Etherscan API key.
    :return: The ABI of the contract as a dictionary.
    """
    
    url = f"https://api.etherscan.io/v2/api?chainid=1&module=contract&action=getabi&address={contract_address}&apikey={api_key}"
    
    response = requests.get(url)
    response_data = response.json()
    
    print(f"ABI for contract {contract_address} fetched successfully.")

    return response_data["result"]


def save_abi_to_file(abi: dict, contract_address: str, directory: str = ".") -> str:
    """
    Save the ABI to a JSON file named <contract_address>_abi.json.

    :param abi: The ABI object.
    :param contract_address: Used to name the file.
    :param directory: Directory where to save the file.
    :return: Full path to the saved file.
    """
    filename = f"abi.json"
    path = os.path.join(directory, filename)
    with open(path, "w") as f:
        json.dump(abi, f, indent=2)
    print(f"ABI saved to {path}")
    return path


def main():
    contract_address = "0x7BeA39867e4169DBe237d55C8242a8f2fcDcc387"  # Example
    api_key = "MV9CC9PHHNB6WI3CHZWMCEAVDYJTI8MPYP"  # Replace with your Etherscan API key
    try:
        abi = get_contract_abi(contract_address, api_key)
        save_abi_to_file(abi, contract_address)
        print(f"ABI for contract {contract_address} fetched and saved successfully.")
    except Exception as e:
        print(f"Failed to fetch ABI: {e}")



if __name__ == "__main__":
    main()

