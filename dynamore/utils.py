import logging
import os

FORMAT = "[%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
log = logging.getLogger(__name__)
log.setLevel(os.getenv("LOGLEVEL", "INFO"))
