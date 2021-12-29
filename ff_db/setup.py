#!/usr/bin/env python

from distutils.core import setup

setup(
    name="ff_db",
    version="0.1",
    description="Database description for fantasy_football database",
    author="DA BOYZ",
    packages=["ff_db"],
    install_requires=["sqlalchemy", "pandas", "python-dotenv"],
)
