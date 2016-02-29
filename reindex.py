from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch import helpers
import time
import json
import logging
from logging.handlers import RotatingFileHandler
from logging import Formatter
from datetime import datetime
import sys
import argparse

def set_logger():
	logger = logging.getLogger("main")
	log_file_handler = RotatingFileHandler(filename = "./error.log", maxBytes = 10*10*10*1024, backupCount = 50)
	log_file_handler.setLevel(logging.DEBUG)
	log_file_handler.setFormatter(Formatter('%(message)s\n'))
	logger.addHandler(log_file_handler)
	return logger

def get_mappings(index_type, mapping_source):
	
	mapping = {index_type:{}}
	fobj = open(mapping_source)
	for line in fobj.readlines():
		if len(line.strip()) > 0:
			fields_mapping = json.loads(line)
			mapping[index_type]['properties'] = fields_mapping
			break
	return mapping

if __name__ == '__main__':

	logger = set_logger()

	argparser = argparse.ArgumentParser(description='Reindex Elasticsearch', conflict_handler='resolve')
	argparser.add_argument('-h', '--host', default='localhost', help="Elasticsearch host")
	argparser.add_argument('-f', '--file',  help="File with JSON mapping", required=True)
	argparser.add_argument('-a', '--alias', help="Alias for the index", required=True)
	argparser.add_argument('-s', '--source-index', help="Source index", required=True)
	argparser.add_argument('-t', '--target-index', help="Target index (default: source_index_[YYMMDDhhmmss]")
	argparser.add_argument("-y", "--source-type", help="Source type", required=True)
	argparser.add_argument("--target-type", help="Target type")
	argparser.add_argument("-i", "--interactive", action='store_true', help="Confirm before taking action")
	argparser.add_argument("-v", "--verbose", action='store_true', help="Verbose")
	argparser.add_argument("--dry-run", action='store_true', help="Dry run. Reindexing will not be done if this option is set.")

	args = argparser.parse_args()

	if args.target_index == None:
		 args.target_index = args.source_index + "_" + datetime.now().strftime('%Y%m%d%H%M%S')
	if args.target_type == None:
		args.target_type = args.source_type

	if args.verbose:
		print "Elasticsearch host: " + args.host
		print "Source index: " + args.source_index
		print "Target index: " + args.target_index
		print "Source index type:" + args.source_type
		print "Target index type: " + args.target_type

	if args.interactive:
		u_input = raw_input("Continue? (y/n): ")
	else: 
		u_input = 'y'


	if u_input == 'y':
		try:
			elastic_client = Elasticsearch([{"host":host,"port":"9200"}])
			index_client = IndicesClient(elastic_client)
			
			mapping = get_mappings(target_index_type)
			body = None
			if mapping[target_index_type] is not None:
				body = {"mappings": mapping}

			#creating new index with necessory fields mapping
			index_client.create(index=target_index, body=body) #, master_timeout=10, timeout=10

			# reindexind data from source index to target index
			helpers.reindex(client=elastic_client, source_index=source_index, target_index=target_index)
			
			#creating alias for target index
			alias = {'actions': []}
			remove_action = {"remove": {"index": source_index, "alias": index_alias}}
			add_action = {"add": {"index": target_index, "alias": index_alias}}
			alias['actions'].append(remove_action)
			alias['actions'].append(add_action)
			index_client.update_aliases(body=alias)

			#deleteing the source index
			index_client.delete(index=source_index)

		except Exception, e:
			print e
	else:
		print 'cancel'
