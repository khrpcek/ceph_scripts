#!/usr/bin/env python

# Script for a mostly hands off upgrade of a ceph cluster
# Runs a yum -y upgrade; shutdown -r now on each host serially as 
# provided by the input file list. It checks for noout flag, pg, and 
# osd status between hosts. If the cluster is not healthy it does not 
# continue the upgrade until no recovering and degraded PGs.
#
# Use at your own risk. It works well for my clusters.
#
# It assumes you have a ssh key with root access to all of the hosts.


import subprocess
import os
import argparse
import json
import paramiko
import time
import logging


class ceph():
    def __init__(self,args):
        self.cluster = args.cluster
        self.id = args.id
        self.keyring = args.keyring
        self.conf = args.conf

    def check_pg_stat(self):
        #return true false if there are undersized or degraded pg
        self.degraded = False
        self.undersized = False
        pg_stat_json=subprocess.check_output(["ceph --cluster {3} --id {0} -c {1} --format json pg stat".format(self.id, self.conf, self.keyring, self.cluster)],shell=True)
        pg_stat_dict=json.loads(pg_stat_json)
        ##print(pg_stat_dict['pg_summary']['num_pg_by_state'])
        if any('undersized' in d['name'] for d in pg_stat_dict['pg_summary']['num_pg_by_state']):
            self.undersized = True
        if any('degraded' in d['name'] for d in pg_stat_dict['pg_summary']['num_pg_by_state']):
            self.degraded = True

    def check_noout(self):
        health = subprocess.check_output(["ceph --cluster {3} --id {0} -c {1} --format json -s".format(self.id, self.conf, self.keyring, self.cluster)],shell=True)
        health_dict = json.loads(health)
        if 'OSDMAP_FLAGS' in health_dict['health']['checks'].keys():
            if 'noout' in health_dict['health']['checks']['OSDMAP_FLAGS']['summary']['message']:
                self.noout = True
            else:
                self.noout = False
        else:
            self.noout = False

    def check_osd_up(self):
        #false if too many osd are down
        #true if safe to continue
        osd = subprocess.check_output(["ceph --cluster {3} --id {0} -c {1} --format json osd stat".format(self.id, self.conf, self.keyring, self.cluster)],shell=True)
        osd_dict = json.loads(osd)
        allowed = 3
        #for 14.2.11
        if 'osdmap' in osd_dict.keys():
            if osd_dict['osdmap']['num_osds'] - osd_dict['osdmap']['num_up_osds'] > allowed or osd_dict['osdmap']['num_osds'] - osd_dict['osdmap']['num_in_osds'] > allowed:
                logging.warning('Too many osd down to continue')
                self.osd_state = False
            else:
                self.osd_state = True

        #14.2.7 and before, different json output
        if 'osdmap' not in osd_dict.keys():
            if osd_dict['num_osds'] - osd_dict['num_up_osds'] > allowed or osd_dict['num_osds'] - osd_dict['num_in_osds'] > allowed:
                logging.warning('Too many osd down to continue')
                self.osd_state = False
            else:
                self.osd_state = True
    
def yum_upgrade(node):
    #runs a yum upgrade on a node through ssh
    logging.info('Running yum upgrade on %s' % node)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy)
    ssh.connect(node, username='root')
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command('yum -y upgrade;shutdown -r now')
    for l in ssh_stdout.readlines():
        print(l.strip())
    ssh.close()
    logging.info('Upgrade complete. Rebooting %s and waiting for 2 minutes.' % node)
    time.sleep(120)

def read_server_list(fn):
    with open(fn, 'r') as f:
        return([x.strip() for x in f.readlines()])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ceph cluster upgrader')
    parser.add_argument('-C','--conf', help='ceph.conf file, defaults to /etc/ceph/ceph.conf.')
    parser.add_argument('-id','--id', help='Ceph authx user',required=True)
    parser.add_argument('-k','--keyring', help='Path to ceph keyring if not in /etc/ceph/client.\$id.keyring')
    parser.add_argument('--cluster')
    parser.add_argument('-f', help='File containing hosts to upgrade. One host per line')
    args = parser.parse_args()
    
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',datefmt='%Y-%m-%d %H:%M:%S',level=logging.INFO)

    cluster = ceph(args)
    servers = read_server_list(args.f)
    
    for node in servers:
        cluster.check_pg_stat()
        cluster.check_noout()
        cluster.check_osd_up()

        #loop to check if cluster is healthy enough for an upgrade
        #also check if noout is set
        while cluster.undersized == True or cluster.degraded == True or cluster.osd_state == False or cluster.noout == False:
            logging.warning('Cluster not ready to continue. Waiting for 5 min to try see if health improves.')
            if cluster.noout == False:
                logging.warning('noout is not set on the cluster. Please set it to continue')
            if cluster.undersized or cluster.degraded:
                logging.warning('There are undsersized or degraded PGs. Cannot continue until there are none.')
            if cluster.osd_state == False:
                logging.warning('There are too many OSDs down to continue.')
            time.sleep(300)
            cluster.check_pg_stat()
            cluster.check_noout()
            cluster.check_osd_up()

        #one last check before doing an upgrade
        if cluster.undersized == False and cluster.degraded == False and cluster.osd_state == True and cluster.noout == True:
            yum_upgrade(node)

