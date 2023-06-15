from typing import Dict

import yaml


def read_yaml_config(yaml_config: str) -> Dict:
    with open(yaml_config, "r") as config_file:
        pipeline_config = yaml.safe_load(config_file)

        return pipeline_config
