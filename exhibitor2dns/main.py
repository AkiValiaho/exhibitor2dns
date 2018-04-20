#!/usr/bin/env python
"""exhibitor2dns: Dynamic DNS for Exhibitor-run Zookeeper ensembles."""

import argparse
import boto3
import logging
import requests


def parse_args():
    """Parse commandline args."""
    parser = argparse.ArgumentParser(description=__doc__)
    required = parser.add_argument_group('Required flags')
    required.add_argument(
        '--zone', required=True, type=str,
        help='DNS zone name (e.g. prod.example.com)')
    required.add_argument(
        '--rr', type=str, required=True,
        help='Name of A record to manage. '
             'Concatenated with the value of --zone unless it ends in a "."')
    required.add_argument(
        '--exhibitor_url', required=True, metavar='URL', type=str,
        help='Base URL to exhibitor http endpoint '
             '(e.g. http://exhibitor.prod.example.com/)')
    parser.add_argument(
        '--ttl', default=300, type=int,
        help='Default record TTL (default: %(default)s)')
    parser.add_argument(
        '--verbosity', default=20, type=int, metavar='N',
        help='Log level (default: %(default)s)')
    return parser.parse_args()


def get_zk_servers(exhibitor_url):
    """Query Exhibitor's REST api and get the current list of servers."""
    headers = {'accept': 'application/json'}
    url = ''.join([exhibitor_url.rstrip('/'), '/exhibitor/v1/cluster/list'])
    return sorted(requests.get(url, headers=headers).json()['servers'])


def main():
    """main"""
    args = parse_args()
    logging.basicConfig(level=args.verbosity)

    client = boto3.client('route53')
    zone = client.list_hosted_zones_by_name(DNSName=args.zone)
    hosted_zone_id = zone.get('HostedZones')[0].get('Id')

    if args.rr[-1] == '.':
        target_fqdn = args.rr
    else:
        target_fqdn = '%s.%s.' % (args.rr, args.zone)

    logging.info('Managing route53 record: %s', target_fqdn)

    exhibitor_list = get_zk_servers(args.exhibitor_url)
    logging.info('Exhibitor cluster: %s', exhibitor_list)

    existing_record = fetch_existing_resource_records(client, hosted_zone_id, target_fqdn)

    if existing_record:
        logging.info('Existing record: %s', existing_record)
        if sorted(exhibitor_list) != sorted(existing_record):
            logging.info('Updating record to match')
            upsert_record(client, hosted_zone_id, target_fqdn, exhibitor_list, args.ttl)
        else:
            logging.info('Up to date.')
    else:
        logging.info('Creating new record.')
        upsert_record(client, hosted_zone_id, target_fqdn, exhibitor_list, args.ttl)

    for i, zkserver_ip in enumerate(sorted(exhibitor_list)):
        idx = i + 1
        target_fqdn = "zk%02d.%s." % (idx, args.zone)
        logging.info("target_fqdn: %s ip: %s" % (target_fqdn, zkserver_ip))

        ip_list = [zkserver_ip]
        existing_record = fetch_existing_resource_records(client, hosted_zone_id, target_fqdn)

        if existing_record:
            logging.info('Existing record: %s', existing_record)
            if sorted(ip_list) != sorted(existing_record):
                logging.info('Updating record to match')
                upsert_record(client, hosted_zone_id, target_fqdn, ip_list, args.ttl)
            else:
                logging.info('Up to date.')
        else:
            logging.info('Creating new record: %s' % target_fqdn)
            upsert_record(client, hosted_zone_id, target_fqdn, ip_list, args.ttl)

    logging.info('Done!')


def upsert_record(client, hosted_zone_id, target_fqdn, ip_list, ttl):
    try:
        resource_records = []
        for value in ip_list:
            resource_records.append({'Value': value})

        if resource_records:
            client.change_resource_record_sets(
                HostedZoneId=hosted_zone_id,
                ChangeBatch={
                    'Changes': [{
                        'Action': 'UPSERT',
                        'ResourceRecordSet': {
                            'Name': target_fqdn,
                            'Type': 'A',
                            'TTL': ttl,
                            'ResourceRecords': resource_records
                        }
                    }]
                }
            )
    except Exception as e:
        logging.exception(e)


def fetch_existing_resource_records(client, hosted_zone_id, target_fqdn):
    res = client.list_resource_record_sets(HostedZoneId=hosted_zone_id, StartRecordName=target_fqdn, StartRecordType='A')

    resource_records = []
    for record_set in res.get('ResourceRecordSets', []):
        record_set_name = record_set.get('Name', '')
        if(record_set_name != target_fqdn):
            continue

        for record in record_set.get('ResourceRecords', []):
            value = record.get('Value')
            if value is not None:
                resource_records.append(value)

    return sorted(resource_records)


if __name__ == '__main__':
    main()
