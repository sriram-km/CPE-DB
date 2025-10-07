"""Configuration parser for CPE Database project."""

import configparser
import os


class Config:
    """Configuration handler for the CPE database application."""

    def __init__(self, config_file='config.properties'):
        self.config = configparser.ConfigParser()
        self.config_file = config_file
        self._load_config()

    def _load_config(self):
        """Load configuration from properties file."""
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Configuration file {self.config_file} not found")

        # Read the properties file
        with open(self.config_file, 'r') as f:
            config_string = '[DEFAULT]\n' + f.read()

        self.config.read_string(config_string)

    @property
    def es_host(self):
        return self.config.get('DEFAULT', 'es.host')

    @property
    def es_port(self):
        return self.config.getint('DEFAULT', 'es.port')

    @property
    def es_scheme(self):
        return self.config.get('DEFAULT', 'es.scheme')

    @property
    def es_index(self):
        return self.config.get('DEFAULT', 'es.index')

    @property
    def nvd_feed_url(self):
        return self.config.get('DEFAULT', 'nvd.feed.url')

    @property
    def nvd_extract_dir(self):
        return self.config.get('DEFAULT', 'nvd.feed.extract.dir')

    @property
    def es_url(self):
        return f"{self.es_scheme}://{self.es_host}:{self.es_port}"
