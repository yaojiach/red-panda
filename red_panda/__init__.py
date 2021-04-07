__version__ = "1.0.2"

import logging
from logging import NullHandler

logging.getLogger(__name__).addHandler(NullHandler())

from red_panda.red_panda import RedPanda
