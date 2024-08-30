# logmatching
This small script allows the matching of different CMS monitoring sources

# Usage
1) Adapt the configs to your needs
2) `$ export CERN_BEARER_TOKEN=<YOUR-TOKEN>`
3) `$ python3 main.py --config config.ini`

The available examples are for querying data from a subsite of the German T1. 

# TODOs
- add query interval for argparse for automatization
- add OpenSearch pushing
- add OpenSearch docker stack
- add proper error handling for matching
