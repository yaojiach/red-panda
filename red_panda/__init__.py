__version__ = "1.0.0"

import logging


logging.basicConfig(
    level=logging.INFO,
    format="🐼 RedPanda | %(asctime)s | %(name)s | %(levelname)s | %(message)s",
)

from red_panda.red_panda import RedPanda
