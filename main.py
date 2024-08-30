import requests
import os
import json
import argparse
import configparser
import logging

from collections import defaultdict
from typing import Tuple, Dict, List

###################################################################################
# Header for the query
url = 'https://monit-grafana.cern.ch/api/datasources/proxy/9668/_msearch'
headers = {
    'Content-Type': 'application/x-ndjson',  # Change to x-ndjson for newline-delimited JSON
    'Authorization': f'Bearer {os.getenv("CERN_BEARER_TOKEN")}'
}
###################################################################################


###################################################################################
# helper functions
###################################################################################
def parse_config_file(config_file: str) -> Tuple[Dict[str, str | List[str]], Dict[str, str | List[str]]]:
    """
    This function is used to parse a configuration file.

    Parameters:
    config_file (str): The path to the configuration file to be parsed.

    Returns:
    tuple: A tuple where each element is a dictionary representing configurations.
    The dictionary keys include 'fields', 'selection', 'gte', 'lte' and 'index'.

    Note:
    'fields' is a list of OpenSearch fields extracted by splitting the comma-separated string from the configuration
    file. 'selection', 'gte', 'lte' and 'index' are strings and describe the job selection, the time range (gte, lte)
    and the DB index.

    Two configurations, 'htc' and 'wma', corresponding to the indices, are parsed from the file and returned as part of
    the tuple.

    Example usage:
    `config_htc, config_wma = parse_config_file("path/to/config_file")`
    """
    # Initialize the ConfigParser
    config = configparser.ConfigParser()
    # Read the config file
    config.read(config_file)

    logger.info('Reading config')
    # Extract keys
    config_htc = dict(fields=config.get('htc', 'fields').split(', '), selection=config.get('htc', 'selection'),
                      gte=config.get('htc', 'gte'), lte=config.get('htc', 'lte'), index=config.get('htc', 'index'))
    config_wma = dict(fields=config.get('wma', 'fields').split(', '), selection=config.get('wma', 'selection'),
                      gte=config.get('wma', 'gte'), lte=config.get('wma', 'lte'), index=config.get('wma', 'index'))
    logger.info(f'Parsed config (htc): {config_htc}')
    logger.info(f'Parsed config (wma): {config_wma}')
    return config_htc, config_wma


def read_selection_json(path: str) -> Dict:
    """
    This function reads and parses a JSON file containing the selection for OpenSearch from the provided path.

    Args:
        path (str): A string referring to the file path of the JSON file.

    Returns:
        dict: The parsed JSON file as a dictionary.

    Note:
        If the file is not found, the program will exit with a specific error message.
    """
    try:
        with open(path, 'r') as file:
            logger.info(f'Selection file loaded: {file}')
            return json.load(file)
    except FileNotFoundError:
        exit(f'Config error. {path} not found')


def create_query_head(config: Dict) -> str:
    """
        This function forms a query header for OpenSearch based on the provided configuration.

        Args:
            config (dict): A dictionary with configuration settings where the 'index' key refers to the Elasticsearch
                           index.

        Returns:
            str: A query header as a JSON string + "\n".
    """
    head = json.dumps({
        'search_type': 'query_then_fetch',
        'ignore_unavailable': True,
        'index': [f'{config["index"]}']
    })
    logger.debug(f'Head created: {head}')
    return head


def create_query_body(config: Dict) -> str:
    """
     This function forms a query body for OpenSearch based on the provided configuration.

     Args:
         config (dict): A dictionary with configuration settings which includes fields such as 'fields', 'selection',
                        'gte' and 'lte'.
         'fields': list of fields to be returned.
         'selection': file path to the json file containing the selection criteria.
         'gte' and 'lte' are the range endpoints for the 'metadata.timestamp' field.

     Returns:
         str: The query body as a JSON string + "\n".

     Note:
         If the config dictionary does not contain necessary keys, it may cause a KeyError.
     """
    body = json.dumps({
        "sort": [
            {
                "data.RecordTime": {
                    "order": "desc",
                    "unmapped_type": "boolean"
                }
            }
        ],
        "size": 500,
        "version": True,
        "aggs": {
            "2": {
                "date_histogram": {
                    "field": "data.RecordTime",
                    "fixed_interval": "30s",
                    "time_zone": "UTC",
                    "min_doc_count": 1
                }
            }
        },
        "_source": {
            "includes": config["fields"]
        },
        "query": {
            "bool": {
                "must": read_selection_json(config["selection"]),
                "filter": [
                    {
                        "range": {
                            "metadata.timestamp": {
                                'gte': config["gte"],
                                'lte': config["lte"]
                            }
                        }
                    }
                ],
            },
        }
    })
    logger.debug(f'Body created: {body}')
    return body


def fetch_data(url: str, headers: Dict, query: str):
    """
    This function is used to post a request to a specified URL and return the data from the response.

    Parameters:
    url (str): The base URL where the request is sent. It is defined at the beginning of the script.
    headers (str): The headers to be included in the request. (JSON string + \n)
    query (str): The data to be included in the body of the request. It is build from the config. (JSON string + \n)

    Returns:
    dict: A dictionary representing part of the response returned from the server.
    Specifically, it returns the 'hits' field of the first response (in a list of responses).

    It makes a POST request to the specific 'url' with the headers 'header' and data 'query'.
    If the status code of the response if not 200, the function will print an error message and exit.
    """
    secure_headers = headers.copy()
    secure_headers['Authorization'] = 'Bearer <HIDDEN>'
    logger.info(f'Initiating request:\n###########\nurl:{url},\nheader:{secure_headers},\nquery: {query}###########')

    response = requests.post(url, headers=headers, data=query)
    status_code = response.status_code
    logger.info(f'Request status: {status_code}')
    # logger.debug(f'Response: {response.json()}')  # SPAM
    if status_code != 200:
        logger.error(f'failed! ({status_code})')
        exit()
    return response.json()["responses"][0]["hits"]


###################################################################################
# Log creation and matching FTW:
###################################################################################
def add_log_htc(data: list) -> list[dict]:
    """
    This function processes a list of data entries, creates a specialized `EOSLogURL` for each entry,
    and returns a list of modified entries with added EOSLogURLs. The idea for the log matching is that from both
    indices all information are available to assemble the log URL. With this, a matching of the different data sources
    can be accomplished!

    Parameters:
    data (list): A list of data entries. Each entry is a dictionary containing at least the `_source` key,
    which itself is a dictionary that includes `data`, and at least the necessary keys for building the URL:
    `WMAgent_SubTaskName`, `ScheddName`, and `Args`.

    Returns:
    list: A list of the modified data entries. Each entry includes a newly created `EOSLogURL`.

    The `EOSLogURL` is constructed using `WMAgent_SubTaskName`, `ScheddName`, and `Args`.

    Example usage:
    `new_data = add_log_htc(old_data)`

    Note:
    This function assumes a specific structure for the input data that should originate from a python requests response.
    If the input data does not have this structure, the function may not work as expected. Make sure the input data
    includes all the required fields.
    """
    result = []
    for entry in data:
        args = entry["_source"]["data"]["Args"].split(" ")
        entry["_source"]['data']["EOSLogURL"] = "https://eoscmsweb.cern.ch/eos/cms/store/logs/prod/recent/PRODUCTION" + \
                                                entry["_source"]["data"]["WMAgent_SubTaskName"] + "/" + \
                                                entry["_source"]["data"]["ScheddName"] + "-" + args[1] + "-" + \
                                                args[2] + "-log.tar.gz"
        result.append(entry["_source"])
    return result


def add_log_wma(data: list) -> list[dict]:  # do it for all!
    """
    Function to construct and add the `EOSLogURL` to the wmarchive logs, if not available.
    Minimum required fields: data.task, data.meta_data.host, data.meta_data.fwjr_id

    More detailed description available for `add_log_htc()`
    """
    result = []
    for entry in data:
        # assembling matching EOSlog, if unavailable:
        if entry["_source"]['data'].get("EOSLogURL", "") == "":
            logger.debug(f'No log found, create!')
            entry["_source"]['data'][
                "EOSLogURL"] = "https://eoscmsweb.cern.ch/eos/cms/store/logs/prod/recent/PRODUCTION" + \
                               entry["_source"]["data"]["task"] + "/" + entry["_source"]["data"]["meta_data"][
                                   "host"] + "-" + \
                               entry["_source"]["data"]["meta_data"]["fwjr_id"] + "-log.tar.gz"
        result.append(entry["_source"])
    return result


def merge_dicts_by_url(dict_list: list) -> tuple[list[dict[str, dict]], list[dict[str, dict]]]:
    """
        This function merges dictionary entries in a list by a common 'EOSLogURL'.
        If there's an entry without a match, it is saved separately.
        This function assumes a specific structure for the input data, i.e., each dictionary in dict_list should
        contain at least a 'data' key which includes an 'EOSLogURL' key that may was created before with the according
        function (add_log_*()).

        Parameters:
        dict_list (list): A list of dictionaries. Each dictionary contains at least a 'data' key,
        which includes an 'EOSLogURL' key for matching.

        Returns:
        tuple: A tuple of two lists. The first list contains the merged dictionaries
        and the second list contains the single entries.

        Example usage:
        `merged_dicts, single_entries = merge_dicts_by_url(dict_list)`

        Note:
        ++++ THE STRUCTURE IS NOT GUARANTEED TO BE COMPLETELY CONTAINING IN THE CURRENT VERSION!! +++++
        """
    # Group dictionaries by 'EOSLogURL'
    url_groups = defaultdict(list)
    for item in dict_list:
        url = item['data'].get('EOSLogURL')
        url_groups[url].append(item)

    # List to hold merged dictionaries
    merged_dicts = []
    # List to hold non-matching single entries
    single_entries = []

    # Process each group
    for url, group in url_groups.items():
        if len(group) > 1:
            # Initialize a new dictionary to hold the merged data
            merged_dict = {'data': {}}
            for d in group:
                for key, value in d['data'].items():
                    if key in merged_dict['data']:
                        # If the key exists, merge or append the new value
                        if isinstance(merged_dict['data'][key], list):
                            # Append if it's a list
                            merged_dict['data'][key].extend(value if isinstance(value, list) else [value])
                            # Remove duplicates if it's a list of primitives
                            if all(isinstance(x, (str, int, float)) for x in merged_dict['data'][key]):
                                merged_dict['data'][key] = list(set(merged_dict['data'][key]))
                        elif isinstance(merged_dict['data'][key], dict):
                            # Merge dictionaries recursively
                            merged_dict['data'][key].update(value)
                        else:
                            # Replace the value if it's not a list or dict
                            merged_dict['data'][key] = value
                    else:
                        # Add the new key-value pair
                        merged_dict['data'][key] = value
            merged_dicts.append(merged_dict)
        else:
            # Add to single entries if there's only one dict in the group
            single_entries.append(group[0])
    # logger.debug(f'No matches found:\n{single_entries}')  # SPAM
    return merged_dicts, single_entries


###################################################################################
# Main
###################################################################################
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some fields and selection JSON.')
    parser.add_argument('--config', type=str, help='Path to the configuration file')
    parser.add_argument('--logtofile', type=str, help='Specify filename to log to file')
    parser.add_argument('--loglevel', type=str, default='WARNING', help='Specify log level [WARNING]')
    #parser.add_argument('--interval', type=int, default=900, help='Interval for the query in seconds. \n'
    #                                        ' NOTE: the interval should match the htc interval to avoid duplicates! ')
    args = parser.parse_args()

    # setup logging
    logger = logging.getLogger('my_logger')
    logger.setLevel(args.loglevel)

    # Create console handler and set level to debug
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)  # Always log to console

    # If a filename is provided, also log to file
    if args.logtofile:
        file_handler = logging.FileHandler(args.logtofile)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        logger.addHandler(file_handler)

    # parse config
    config_htc, config_wma = parse_config_file(args.config)

    # create heads
    query_head_htc = create_query_head(config_htc) + '\n'
    query_head_wma = create_query_head(config_wma) + '\n'

    # create bodies
    query_body_htc = create_query_body(config_htc) + '\n'
    query_body_wma = create_query_body(config_wma) + '\n'

    # Combine headers and both parts of the request into a single request JSON string
    query_wma = query_head_wma + query_body_wma
    query_htc = query_head_htc + query_body_htc

    # fetch data
    wma = fetch_data(url, headers, query_wma)
    htc = fetch_data(url, headers, query_htc)

    # n queried:
    nqueried_wma = wma["total"]["value"]
    nqueried_htc = htc["total"]["value"]

    if nqueried_wma == 0:
        exit('WMAgent: No data available!')
    if nqueried_htc == 0:
        exit('HTCondor: No data available!')

    logger.info(f'N wma: {nqueried_wma}')
    logger.info(f'N htc:{nqueried_htc}')

    # prepare data for merging
    wma_with_logs = add_log_wma(wma["hits"])
    htc_with_logs = add_log_htc(htc["hits"])

    # merge
    merged_dicts, single_entries = merge_dicts_by_url(htc_with_logs + wma_with_logs)
    # logger.debug(f'merged: {merged_dicts}')  # SPAM
    # logger.debug(f'single: {single_entries}')  # SPAM

    for ent in merged_dicts:
        logger.debug(f'Merged:\n{json.dumps(ent, indent=2)}')
    for ent in single_entries:
        logger.debug(f'No match:\n{json.dumps(ent, indent=2)}')

    logger.info(f'Statistics:\n##########################################')
    logger.info(f'N merged: {len(merged_dicts)}')
    logger.info(f'N no match: {len(single_entries)}\n##########################################')

    # Processing
    ### DO WHATEVER YOU WANT ###

