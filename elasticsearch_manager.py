"""Elasticsearch setup and management for CPE Database."""

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
import logging

logger = logging.getLogger(__name__)


class ElasticsearchManager:
    """Manages Elasticsearch connection and index operations."""

    def __init__(self, config):
        self.config = config
        self.es = Elasticsearch([config.es_url])
        self.index_name = config.es_index

    def test_connection(self):
        """Test Elasticsearch connection."""
        try:
            info = self.es.info()
            logger.info(f"Connected to Elasticsearch: {info['version']['number']}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
            return False

    def create_index(self):
        """Create the CPE index with proper mapping."""
        mapping = {
            "mappings": {
                "properties": {
                    "cpeName": {"type": "keyword"},
                    "cpeNameId": {"type": "keyword"},
                    "created": {"type": "date"},
                    "lastModified": {"type": "date"},
                    "deprecated": {"type": "boolean"},
                    "refs": {
                        "type": "nested",
                        "properties": {
                            "ref": {"type": "keyword"},
                            "type": {"type": "keyword"}
                        }
                    },
                    "titles": {
                        "type": "nested",
                        "properties": {
                            "lang": {"type": "keyword"},
                            "title": {
                                "type": "text",
                                "analyzer": "standard",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    },
                                    "raw": {
                                        "type": "keyword"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        try:
            if self.es.indices.exists(index=self.index_name):
                logger.info(f"Index {self.index_name} already exists")
                return True

            self.es.indices.create(index=self.index_name, body=mapping)
            logger.info(f"Created index {self.index_name}")
            return True
        except RequestError as e:
            logger.error(f"Failed to create index: {e}")
            return False

    def delete_index(self):
        """Delete the CPE index."""
        try:
            if self.es.indices.exists(index=self.index_name):
                self.es.indices.delete(index=self.index_name)
                logger.info(f"Deleted index {self.index_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete index: {e}")
            return False

    def bulk_index_documents(self, documents):
        """Bulk index documents to Elasticsearch."""
        from elasticsearch.helpers import bulk

        actions = []
        for doc in documents:
            actions.append({
                "_index": self.index_name,
                "_source": doc
            })

        try:
            bulk(self.es, actions)
            logger.info(f"Indexed {len(documents)} documents")
            return True
        except Exception as e:
            logger.error(f"Failed to bulk index documents: {e}")
            return False

    def get_index_stats(self):
        """Get index statistics."""
        try:
            stats = self.es.indices.stats(index=self.index_name)
            return stats['indices'][self.index_name]['total']['docs']['count']
        except Exception as e:
            logger.error(f"Failed to get index stats: {e}")
            return 0

    def recreate_index(self):
        """Delete and recreate the index with updated mapping."""
        logger.info("Recreating index with updated mapping...")
        self.delete_index()
        return self.create_index()
