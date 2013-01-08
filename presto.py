#!/usr/bin/env python

""" Presto Console Launcher
"""
import os
import sys

import json
import glob
import urllib2
import subprocess

from random import shuffle

PRESTO_JAR_NAME = 'presto-standalone.jar'
SUGGESTED_JDK_RPM = 'fb-jdk_7u10-64'
SUGGESTED_JAVA_PATH = '/usr/local/jdk-7u10-64/bin/java'
JDK_PATH_PREFIX = '/usr/local/jdk-7u'
JDK_PATH_SUFFIX = '-64'

# Fixed SMC tiers
AIRLIFT_COORDINATOR_FRC_TIER = 'airship-coordinator-frc'
AIRLIFT_COORDINATOR_SNC_TIER = 'airship-coordinator-snc'

# namespace => (airship_tier, hive_database_name)
NAMESPACES = {
    'ads': (AIRLIFT_COORDINATOR_SNC_TIER, 'ads'),
    'ads_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'ads_carolina'),
    'ads_mercury': (AIRLIFT_COORDINATOR_FRC_TIER, 'ads_mercury'),
    'anon': (AIRLIFT_COORDINATOR_FRC_TIER, 'anon'),
    'bi': (AIRLIFT_COORDINATOR_SNC_TIER, 'bi'),
    'bi_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'bi_carolina'),
    'cea': (AIRLIFT_COORDINATOR_FRC_TIER, 'cea'),
    'cea_platinum': (AIRLIFT_COORDINATOR_FRC_TIER, 'cea_platinum'),
    'cea_west': (AIRLIFT_COORDINATOR_SNC_TIER, 'cea_west'),
    'coefficient': (AIRLIFT_COORDINATOR_SNC_TIER, 'coefficient'),
    'coefficient_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'coefficient_carolina'),
    'datascience': (AIRLIFT_COORDINATOR_SNC_TIER, 'datascience'),
    'datascience_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'datascience_carolina'),
    'default': (AIRLIFT_COORDINATOR_SNC_TIER, 'default'),
    'default_freon': (AIRLIFT_COORDINATOR_FRC_TIER, 'default_freon'),
    'default_platinum': (AIRLIFT_COORDINATOR_FRC_TIER, 'default_platinum'),
    'desktop': (AIRLIFT_COORDINATOR_SNC_TIER, 'desktop'),
    'desktop_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'desktop_carolina'),
    'di': (AIRLIFT_COORDINATOR_SNC_TIER, 'di'),
    'di_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'di_carolina'),
    'di_mercury': (AIRLIFT_COORDINATOR_FRC_TIER, 'di_mercury'),
    'entities': (AIRLIFT_COORDINATOR_SNC_TIER, 'entities'),
    'entities_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'entities_carolina'),
    'feed': (AIRLIFT_COORDINATOR_SNC_TIER, 'feed'),
    'feed_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'feed_carolina'),
    'finance': (AIRLIFT_COORDINATOR_SNC_TIER, 'finance'),
    'finance_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'finance_carolina'),
    'groupsmessaging': (AIRLIFT_COORDINATOR_SNC_TIER, 'groupsmessaging'),
    'groupsmessaging_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'groupsmessaging_carolina'),
    'growth': (AIRLIFT_COORDINATOR_SNC_TIER, 'growth'),
    'growth_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'growth_carolina'),
    'hr': (AIRLIFT_COORDINATOR_SNC_TIER, 'hr'),
    'hr_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'hr_carolina'),
    'identity': (AIRLIFT_COORDINATOR_SNC_TIER, 'identity'),
    'incrementalscraping': (AIRLIFT_COORDINATOR_FRC_TIER, 'incrementalscraping'),
    'infrastructure': (AIRLIFT_COORDINATOR_SNC_TIER, 'infrastructure'),
    'infrastructure_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'infrastructure_carolina'),
    'it': (AIRLIFT_COORDINATOR_SNC_TIER, 'it'),
    'it_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'it_carolina'),
    'legal': (AIRLIFT_COORDINATOR_SNC_TIER, 'legal'),
    'legal_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'legal_carolina'),
    'listscribe': (AIRLIFT_COORDINATOR_SNC_TIER, 'listscribe'),
    'listscribe_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'listscribe_carolina'),
    'listscribe_tmp_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'listscribe_tmp_carolina'),
    'locations': (AIRLIFT_COORDINATOR_SNC_TIER, 'locations'),
    'locations_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'locations_carolina'),
    'locations_tmp_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'locations_tmp_carolina'),
    'measurementsystems': (AIRLIFT_COORDINATOR_FRC_TIER, 'measurementsystems'),
    'measurementsystems_tmp_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'measurementsystems_tmp_carolina'),
    'measurementsystems_west': (AIRLIFT_COORDINATOR_SNC_TIER, 'measurementsystems_west'),
    'mobile': (AIRLIFT_COORDINATOR_SNC_TIER, 'mobile'),
    'mobile_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'mobile_carolina'),
    'networkego': (AIRLIFT_COORDINATOR_SNC_TIER, 'networkego'),
    'networkego_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'networkego_carolina'),
    'notifications': (AIRLIFT_COORDINATOR_SNC_TIER, 'notifications'),
    'notifications_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'notifications_carolina'),
    'operations': (AIRLIFT_COORDINATOR_SNC_TIER, 'operations'),
    'operations_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'operations_carolina'),
    'pages': (AIRLIFT_COORDINATOR_SNC_TIER, 'pages'),
    'pages_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'pages_carolina'),
    'payments': (AIRLIFT_COORDINATOR_SNC_TIER, 'payments'),
    'payments_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'payments_carolina'),
    'photos': (AIRLIFT_COORDINATOR_SNC_TIER, 'photos'),
    'photos_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'photos_carolina'),
    'platform': (AIRLIFT_COORDINATOR_SNC_TIER, 'platform'),
    'platform_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'platform_carolina'),
    'productanalytics': (AIRLIFT_COORDINATOR_SNC_TIER, 'productanalytics'),
    'productanalytics_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'productanalytics_carolina'),
    'profiletimeline': (AIRLIFT_COORDINATOR_SNC_TIER, 'profiletimeline'),
    'profiletimeline_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'profiletimeline_carolina'),
    'search': (AIRLIFT_COORDINATOR_SNC_TIER, 'search'),
    'search_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'search_carolina'),
    'search_tmp_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'search_tmp_carolina'),
    'security': (AIRLIFT_COORDINATOR_SNC_TIER, 'security'),
    'security_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'security_carolina'),
    'si': (AIRLIFT_COORDINATOR_SNC_TIER, 'si'),
    'si_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'si_carolina'),
    'tmp_cea_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'tmp_cea_carolina'),
    'tmp_entities': (AIRLIFT_COORDINATOR_FRC_TIER, 'tmp_entities'),
    'tmp_growth_tmp_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'tmp_growth_tmp_carolina'),
    'tmp_operations_tmp_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'tmp_operations_tmp_carolina'),
    'userexperience': (AIRLIFT_COORDINATOR_SNC_TIER, 'userexperience'),
    'userexperience_carolina': (AIRLIFT_COORDINATOR_FRC_TIER, 'userexperience_carolina'),
}

def run_command(args):
    process = subprocess.Popen(args, stdout=subprocess.PIPE)
    out, err = process.communicate()
    return out

def read_service_inventory(coordinators):
    service_inventory = None
    for host_port in coordinators:
        try:
            f = urllib2.urlopen('http://%s/v1/serviceInventory' % host_port)
            service_inventory = json.loads(f.read())
        except urllib2.URLError:
            pass
    return service_inventory

def select_discovery_service(service_inventory):
    services = service_inventory['services']
    shuffle(services)
    for service in services:
        if service['type'] == 'discovery' and service['state'] == 'RUNNING':
            return service['properties']['http'] 
    return None

def discover_presto_coordinator(discovery_service):
    f = urllib2.urlopen('%s/v1/service/presto-coordinator/general' % discovery_service)
    coordinators = json.loads(f.read())['services']
    if len(coordinators) == 0:
        return None
    shuffle(coordinators)
    return coordinators[0]['properties']['http']

def find_existing_java():
    # Assumes JDK path is of the form "JDK_PATH_PREFIX<version>-JDK_PATH_SUFFIX"
    matching_jdk_paths = glob.glob('%s*%s' % (JDK_PATH_PREFIX, JDK_PATH_SUFFIX))
  
    if len(matching_jdk_paths) == 0:
        return None
  
    # Sort based on JDK version number
    matching_jdk_paths.sort(key = lambda jdk_path: int(jdk_path[len(JDK_PATH_PREFIX):][:-len(JDK_PATH_SUFFIX)]))
  
    # Use the highest JDK version number
    candidate_java = "%s/bin/java" % matching_jdk_paths[-1]
    return candidate_java if os.path.exists(candidate_java) else None

def install_java():
    print "WARNING: Presto requires Java 7 to be installed"
    print
    confirm = raw_input('Would you like to install %s [y/N]?' % SUGGESTED_JDK_RPM)
    if confirm.strip().lower() != 'y':
        sys.exit('Java not installed')
    subprocess.check_call(['sudo', 'yum', 'install', SUGGESTED_JDK_RPM])
    print
    if not os.path.exists(SUGGESTED_JAVA_PATH):
        sys.exit('ERROR: Java not found after install: %s' % SUGGESTED_JAVA_PATH)
    return SUGGESTED_JAVA_PATH

def fetch_presto_coordinator(airlift_coordinator_tier):
    coordinators = run_command(['smcc', 'list-services', airlift_coordinator_tier]).rstrip('\n').split('\n')
    service_inventory = read_service_inventory(coordinators)
    if service_inventory == None:
        sys.exit('Unable to locate a valid Airship coordinator') 
    discovery_service = select_discovery_service(service_inventory)
    if discovery_service == None:
        sys.exit('Unable to locate Discovery')
    presto_coordinator = discover_presto_coordinator(discovery_service)
    if presto_coordinator == None:
        sys.exit('Unable to locate a Presto coordinator') 
    return presto_coordinator

def launch_console(parent_dir, namespace, additional_args):
    if namespace == 'presto':
        # presto is a reserved namespace for local presto data
        catalog = 'default'
        schema = 'default'
        airlift_coordinator_tier = AIRLIFT_COORDINATOR_FRC_TIER
    elif namespace in NAMESPACES:
        catalog = 'hive'
        (airlift_coordinator_tier, schema) = NAMESPACES[namespace]
    else:
        sys.exit('Unsupported namespace: %s' % namespace)
  
    presto_coordinator = fetch_presto_coordinator(airlift_coordinator_tier)
 
    # Locate Java
    java_path = find_existing_java()
    if java_path == None:
        java_path = install_java()
  
    jar_path = parent_dir + "/" + PRESTO_JAR_NAME
  
    os.execv(java_path, [java_path, '-jar', jar_path, 'console', '--server', presto_coordinator, '--catalog', catalog, '--schema', schema] + additional_args)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Usage: %s <namespace> [optional_flags]' % sys.argv[0])
    launch_console(os.path.abspath(os.path.dirname(os.path.realpath(sys.argv[0]))), sys.argv[1], sys.argv[2:])
