# -*- coding: utf-8 -*-
import os

import pytest

from red_panda import __version__


def test_version():
    assert __version__ == '0.1.0'
