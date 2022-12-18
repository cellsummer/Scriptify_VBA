import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("log_file.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
