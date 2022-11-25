import logging.config
import json

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(filename)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("log_file.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def read_config(config_path: str):

    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # remove descripitive keys from config (starts with _)
    for key in config:
        if key[0] == '_':
            del config[key]
    
    return config

