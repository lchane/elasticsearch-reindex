# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Copyright  2016, Shyam Anand <shyamwdr@gmail.com>


from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch import helpers
import json
import logging
from logging.handlers import RotatingFileHandler
from logging import Formatter
import sys

class Reindexer:

	"Elasticsearch reindexer"

	def __init__(self, host, port, mapping_file, alias, source_index, target_index, source_type, target_type):

		self.__host 		= host
		self.__port 		= port
		self.__mapping_file = mapping_file
		self.__alias 		= alias
		self.__source_index = source_index
		self.__target_index = target_index
		self.__source_type 	= source_type
		self.__target_type 	= target_type

	def get_mappings(self):
	    mapping = {self.__target_type: {}}
	    fobj = open(self.__mapping_file)
	    for line in fobj.readlines():
	        if len(line.strip()) > 0:
	            fields_mapping = json.loads(line)
	            mapping[self.__source_type]['properties'] = fields_mapping
	            break
	    return mapping

	def reindex(self):
		
		elastic_client = Elasticsearch([{"host":self.__host,"port":self.__port}])
		index_client = IndicesClient(elastic_client)

		mapping = self.get_mappings()

		body = None
		if mapping[self.__target_type] is not None:
			body = {"mappings": mapping}

		print body

		# #Create new index with necessory fields mapping
		# index_client.create(index=target_index, body=body) #, master_timeout=10, timeout=10

		# # reindexind data from source index to target index
		# helpers.reindex(client=elastic_client, source_index=source_index, target_index=target_index)

		# #creating alias for target index
		# alias = {'actions': []}
		# remove_action = {"remove": {"index": source_index, "alias": index_alias}}
		# add_action = {"add": {"index": target_index, "alias": index_alias}}
		# alias['actions'].append(remove_action)
		# alias['actions'].append(add_action)
		# index_client.update_aliases(body=alias)

		# #deleteing the source index
		# index_client.delete(index=source_index)


	def getHost(self):
		return self.__host

	def getSourceIndex(self):
		return self.__source_index

	def getTargetIndex(self):
		return self.__target_index

	def getSourceType(self):
		return self.__source_type

	def getTargetType(self):
		return self.__target_type

