#!/usr/bin/python



from elasticsearch import Elasticsearch  
import sys
import urllib
import json
import sys
import ast


try:  
    es = Elasticsearch(['http://blabla:9200'])
except Exception as e:  
    print 'Connection failed.'
    sys.exit(1)

# Print error message in case of unsupported  metric.
def err_message(option, metric):

    print "%s metric is not under support for %s option." % (metric, option)
    sys.exit(1)


def cluster_health(metric):

    result = es.cluster.health()

    return result


def cluster_mem_stats(metric):

    result = es.cluster.stats()
    size = result['nodes']['jvm']['mem'][metric]

    print size


def node_mem_stats(metric):

    node_stats = es.nodes.stats(node_id='_local', metric='jvm')
    node_id = node_stats['nodes'].keys()[0]

    if 'heap_used_percent' in metric:

        result = node_stats['nodes'][node_id]['jvm']['mem'][metric]
        print result
    else:
        if 'pool_young' in metric:

            result = node_stats['nodes'][node_id]['jvm']['mem']['pools']['young']
            size = result['used_in_bytes']
        elif 'pool_old' in metric:

            result = node_stats['nodes'][node_id]['jvm']['mem']['pools']['old']
            size = result['used_in_bytes']
        elif 'pool_survivor' in metric:

            result = node_stats['nodes'][node_id]['jvm']['mem']['pools']['survivor']
            size = result['used_in_bytes']
        else:

            result = node_stats['nodes'][node_id]['jvm']['mem']
            size = result[metric]

        print size


def node_index_stats(metric):

    node_stats = es.nodes.stats(node_id='_local', metric='indices')
    node_id = node_stats['nodes'].keys()[0]

    if metric == 'total_merges_mem':
        result = node_stats['nodes'][node_id]['indices']['merges']
        size = result['total_size_in_bytes']

    if metric == 'total_filter_cache_mem':
        result = node_stats['nodes'][node_id]['indices']['filter_cache']
        size = result['memory_size_in_bytes']

    if metric == 'total_field_data_mem':
        result = node_stats['nodes'][node_id]['indices']['fielddata']
        size = result['memory_size_in_bytes']

    print size



# Definition of checks

cluster_checks = {'active_primary_shards': cluster_health,  
                  'active_shards': cluster_health,
                  'number_of_pending_tasks': cluster_health,
                  'relocating_shards': cluster_health,
                  'status': cluster_health,
                  'unassigned_shards': cluster_health,
                  'number_of_nodes': cluster_health,
                  'heap_max_in_bytes': cluster_mem_stats,
                  'heap_used_in_bytes': cluster_mem_stats}

node_checks = {'heap_pool_young_gen_mem': node_mem_stats,  
               'heap_pool_old_gen_mem': node_mem_stats,
               'heap_pool_survivor_gen_mem': node_mem_stats,
               'heap_max_in_bytes': node_mem_stats,
               'heap_used_in_bytes': node_mem_stats,
               'heap_used_percent': node_mem_stats,
               'total_filter_cache_mem': node_index_stats,
               'total_field_data_mem': node_index_stats,
               'total_merges_mem': node_index_stats}



node_count=(cluster_checks.get('number_of_nodes')('number_of_nodes'))





min_count=20
ok_status='123'
unassigned_shards_count=0
#list_node = range(01, 15)

error_stat_list=[]
masternode_list={}
def list_data(full_url):

	
	es = Elasticsearch([full_url])
	result=es.cluster.health()

	if (result['status'] != ok_status) and (result['unassigned_shards'] >= unassigned_shards_count) and (result['number_of_nodes'] < min_count):

	    f = urllib.urlopen(full_url+'/_cluster/state')
	    stats = json.loads( f.read() )
	    A = json.load(urllib.urlopen(full_url+'/_cluster/state'))
#	    print stats['master_node']
	    masternode_list[full_url] = stats['master_node']

	    node_num=(" node count: " + "\"" + str(result['number_of_nodes']) + "\"" + ",")
	    clust_name=(" cluster name:  "+ "\"" + result['cluster_name'] + "\", " )
	    stat_val=(full_url+" status: " + "\"" + result['status'] + "\"" + ",")
	    master=("master: "+masternode_list[full_url])
	    info_all=stat_val+node_num+clust_name+master
	    
	    

	    
#	    print masternode_list#.values()
#	    myset = list(set(masternode_list.values()))
#	    print myset

#	    print info_all
#	    print result['unassigned_shards']
#	    print type(stat_val)
#	    print stat_val
	    error_stat_list.append(info_all)
#	    print ', '.join(error_stat_list)
#	    print error_stat_list
#	    print "cluster name are "+result['cluster_name']"
	    error_cluster_name=[]
#	    print "nombers of nodes "+str(result['number_of_nodes'])

def check_cluster(cluster_name,env,list_node):
    for in_list in list_node:
        full_name="http://"+cluster_name+str(in_list).zfill(2)+"."+env+".c:9200"
        list_data(full_name)
        
    if len(error_stat_list) != 0:
	print '\n'.join(error_stat_list)
	del error_stat_list[:]





check_cluster(sys.argv[1],sys.argv[2],sys.argv[3:])












