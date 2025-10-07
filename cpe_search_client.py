"""CPE Search Client for querying the Elasticsearch index."""

import logging
from elasticsearch.exceptions import RequestError

logger = logging.getLogger(__name__)


class CPESearchClient:
    """Client for searching CPE data in Elasticsearch."""

    def __init__(self, elasticsearch_manager):
        self.es_manager = elasticsearch_manager
        self.es = elasticsearch_manager.es
        self.index_name = elasticsearch_manager.index_name

    def search_by_tool_name(self, tool_name, fuzziness="AUTO", size=10):
        """Search CPE entries by tool name - first exact match, then fuzzy with ranking."""

        # First, try exact match search
        exact_query = {
            "query": {
                "nested": {
                    "path": "titles",
                    "query": {
                        "bool": {
                            "should": [
                                {
                                    "match_phrase": {
                                        "titles.title": {
                                            "query": tool_name,
                                            "boost": 10.0  # High boost for exact phrase matches
                                        }
                                    }
                                },
                                {
                                    "term": {
                                        "titles.title.keyword": {
                                            "value": tool_name,
                                            "boost": 15.0  # Highest boost for exact term matches
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            },
            "size": size
        }

        # Execute exact search first
        exact_results = self._execute_search(exact_query, f"exact tool name '{tool_name}'")

        # If we found exact matches, return them
        if exact_results and exact_results.get('total', 0) > 0:
            logger.info(f"Found {exact_results['total']} exact matches for '{tool_name}'")
            return exact_results

        # No exact matches found, fall back to fuzzy search with ranking
        logger.info(f"No exact matches for '{tool_name}', performing fuzzy search")

        fuzzy_query = {
            "query": {
                "nested": {
                    "path": "titles",
                    "query": {
                        "bool": {
                            "should": [
                                {
                                    "match": {
                                        "titles.title": {
                                            "query": tool_name,
                                            "fuzziness": fuzziness,
                                            "boost": 5.0  # Medium boost for fuzzy matches
                                        }
                                    }
                                },
                                {
                                    "wildcard": {
                                        "titles.title": {
                                            "value": f"*{tool_name.lower()}*",
                                            "boost": 3.0  # Lower boost for wildcard matches
                                        }
                                    }
                                },
                                {
                                    "prefix": {
                                        "titles.title": {
                                            "value": tool_name.lower(),
                                            "boost": 4.0  # Higher boost for prefix matches
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            },
            "size": size,
            "sort": [
                {"_score": {"order": "desc"}},  # Sort by relevance score first
                {"lastModified": {"order": "desc"}}  # Then by last modified date
            ]
        }

        fuzzy_results = self._execute_search(fuzzy_query, f"fuzzy tool name '{tool_name}'")

        if fuzzy_results:
            fuzzy_results['query_description'] = f"fuzzy search for tool name '{tool_name}' (no exact matches found)"

        return fuzzy_results

    def search_by_website(self, website, size=10):
        """Search CPE entries by website reference."""
        query = {
            "query": {
                "nested": {
                    "path": "refs",
                    "query": {
                        "wildcard": {
                            "refs.ref": f"*{website}*"
                        }
                    }
                }
            },
            "size": size
        }

        return self._execute_search(query, f"website '{website}'")

    def search_by_exact_cpe(self, cpe_name, size=10):
        """Search by exact CPE name."""
        query = {
            "query": {
                "term": {
                    "cpeName": cpe_name
                }
            },
            "size": size
        }

        return self._execute_search(query, f"exact CPE '{cpe_name}'")

    def search_by_cpe_pattern(self, cpe_pattern, size=10):
        """Search by CPE name pattern (wildcard)."""
        query = {
            "query": {
                "wildcard": {
                    "cpeName": cpe_pattern
                }
            },
            "size": size
        }

        return self._execute_search(query, f"CPE pattern '{cpe_pattern}'")

    def search_by_vendor_product(self, vendor=None, product=None, version=None, size=10):
        """Search by extracting vendor/product/version from CPE format."""
        # CPE format: cpe:2.3:part:vendor:product:version:update:edition:language:sw_edition:target_sw:target_hw:other
        must_clauses = []

        if vendor:
            must_clauses.append({
                "wildcard": {
                    "cpeName": f"*:{vendor}:*"
                }
            })

        if product:
            must_clauses.append({
                "wildcard": {
                    "cpeName": f"*:*:{product}:*"
                }
            })

        if version:
            must_clauses.append({
                "wildcard": {
                    "cpeName": f"*:*:*:{version}:*"
                }
            })

        if not must_clauses:
            logger.error("At least one of vendor, product, or version must be specified")
            return None

        query = {
            "query": {
                "bool": {
                    "must": must_clauses
                }
            },
            "size": size
        }

        search_terms = []
        if vendor:
            search_terms.append(f"vendor '{vendor}'")
        if product:
            search_terms.append(f"product '{product}'")
        if version:
            search_terms.append(f"version '{version}'")

        return self._execute_search(query, f"vendor/product/version: {', '.join(search_terms)}")

    def search_deprecated(self, deprecated=True, size=10):
        """Search for deprecated or non-deprecated CPE entries."""
        query = {
            "query": {
                "term": {
                    "deprecated": deprecated
                }
            },
            "size": size
        }

        status = "deprecated" if deprecated else "non-deprecated"
        return self._execute_search(query, f"{status} entries")

    def search_by_date_range(self, start_date=None, end_date=None, date_field="lastModified", size=10):
        """Search by date range (created or lastModified)."""
        range_query = {}

        if start_date:
            range_query["gte"] = start_date
        if end_date:
            range_query["lte"] = end_date

        if not range_query:
            logger.error("At least one of start_date or end_date must be specified")
            return None

        query = {
            "query": {
                "range": {
                    date_field: range_query
                }
            },
            "size": size
        }

        return self._execute_search(query, f"date range on {date_field}")

    def advanced_search(self, query_dict, size=10):
        """Execute a custom Elasticsearch query."""
        if "size" not in query_dict:
            query_dict["size"] = size

        return self._execute_search(query_dict, "custom query")

    def get_all_documents(self, batch_size=1000):
        """Get all documents from the index for backup purposes."""
        try:
            all_docs = []

            # Use scroll API for large result sets
            query = {
                "query": {"match_all": {}},
                "size": batch_size
            }

            response = self.es.search(
                index=self.index_name,
                body=query,
                scroll='5m'
            )

            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']

            # Add first batch
            all_docs.extend([hit['_source'] for hit in hits])

            # Continue scrolling for remaining documents
            while hits:
                response = self.es.scroll(scroll_id=scroll_id, scroll='5m')
                hits = response['hits']['hits']
                all_docs.extend([hit['_source'] for hit in hits])

            # Clear scroll
            self.es.clear_scroll(scroll_id=scroll_id)

            logger.info(f"Retrieved {len(all_docs)} documents from index")
            return all_docs

        except Exception as e:
            logger.error(f"Failed to retrieve all documents: {e}")
            return []

    def get_statistics(self):
        """Get index statistics and aggregations."""
        try:
            # Basic count
            total_docs = self.es_manager.get_index_stats()

            # Aggregations
            agg_query = {
                "size": 0,
                "aggs": {
                    "deprecated_count": {
                        "terms": {
                            "field": "deprecated"
                        }
                    },
                    "ref_types": {
                        "nested": {
                            "path": "refs"
                        },
                        "aggs": {
                            "types": {
                                "terms": {
                                    "field": "refs.type"
                                }
                            }
                        }
                    },
                    "languages": {
                        "nested": {
                            "path": "titles"
                        },
                        "aggs": {
                            "langs": {
                                "terms": {
                                    "field": "titles.lang"
                                }
                            }
                        }
                    }
                }
            }

            response = self.es.search(index=self.index_name, body=agg_query)

            stats = {
                "total_documents": total_docs,
                "deprecated_breakdown": {},
                "reference_types": {},
                "languages": {}
            }

            # Process aggregations
            aggs = response.get("aggregations", {})

            if "deprecated_count" in aggs:
                for bucket in aggs["deprecated_count"]["buckets"]:
                    stats["deprecated_breakdown"][bucket["key"]] = bucket["doc_count"]

            if "ref_types" in aggs:
                for bucket in aggs["ref_types"]["types"]["buckets"]:
                    stats["reference_types"][bucket["key"]] = bucket["doc_count"]

            if "languages" in aggs:
                for bucket in aggs["languages"]["langs"]["buckets"]:
                    stats["languages"][bucket["key"]] = bucket["doc_count"]

            return stats

        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return None

    def _execute_search(self, query, description):
        """Execute a search query and handle errors."""
        try:
            logger.info(f"Searching for {description}")
            response = self.es.search(index=self.index_name, body=query)

            hits = response.get("hits", {})
            total = hits.get("total", {})

            if isinstance(total, dict):
                total_count = total.get("value", 0)
            else:
                total_count = total

            results = {
                "total": total_count,
                "hits": [hit["_source"] for hit in hits.get("hits", [])],
                "query_description": description
            }

            logger.info(f"Found {total_count} results for {description}")
            return results

        except RequestError as e:
            logger.error(f"Search request error for {description}: {e}")
            return None
        except Exception as e:
            logger.error(f"Search failed for {description}: {e}")
            return None
