import sys
import time
from os import environ

import requests

USER_PLACEHOLDER = "_user_placeholder_"

VPC_RESOURCE = "vpcs"
DROPLETS_RESOURCE = "droplets"
FIREWALLS_RESOURCE = "firewalls"

ID = "id"
NAME = "name"

# full machine slugs reference: https://slugs.do-api.dev/
# https://www.digitalocean.com/blog/premium-droplets-intel-cascade-lake-amd-epyc-rome
CONSUMER_MACHINE_SLUG = "s-2vcpu-2gb-amd"
DB_MACHINE_SLUG = "s-8vcpu-16gb-amd"


def print_and_exit(message):
    print(message)
    sys.exit(1)


DIGITAL_OCEAN_URL = "https://api.digitalocean.com/v2"

VPC_NAME = "eventsql-benchmarks-vpc"
CONSUMER_MACHINE_NAME = "eventsql-benchmarks-consumer-machine"
DB_MACHINE_INSTANCES = int(environ.get("DB_MACHINE_INSTANCES", "3"))
db_machine_names = [f"eventsql-benchmarks-db-machine-{i}" for i in range(DB_MACHINE_INSTANCES)]
FIREWALL_NAME = "eventsql-benchmarks-firewall"

REGION = "fra1"
IMAGE = "ubuntu-24-04-x64"

API_TOKEN = environ.get("DO_API_TOKEN")
if API_TOKEN is None:
    print_and_exit("DO_API_TOKEN env variable needs to be supplied with valid digital ocean token")

SSH_KEY_FINGERPRINT = environ.get("SSH_KEY_FINGERPRINT")
if SSH_KEY_FINGERPRINT is None:
    print_and_exit(
        "SSH_KEY_FINGERPRINT env variable needs to be supplied with ssh key fingerprint that will give you access to droplets!")

AUTH_HEADER = {"Authorization": f"Bearer {API_TOKEN}"}

with open("init_machine.bash") as f:
    INIT_MACHINE_SCRIPT = f.read().replace(USER_PLACEHOLDER, "eventsql")

vpc_config = {
    "name": VPC_NAME,
    "description": "VPC for internal system communication",
    "region": REGION
}


# To debug user data, run:
# cat /var/log/cloud-init-output.log | grep userdata
# ...on the droplet
def consumer_machine_config(vpc_id):
    return {
        "name": CONSUMER_MACHINE_NAME,
        "region": REGION,
        "size": CONSUMER_MACHINE_SLUG,
        "image": IMAGE,
        "ssh_keys": [SSH_KEY_FINGERPRINT],
        "backups": False,
        "ipv6": True,
        "vpc_uuid": vpc_id,
        "monitoring": True,
        "user_data": INIT_MACHINE_SCRIPT,
    }


def db_machine_config(name, vpc_id):
    return {
        "name": name,
        "region": REGION,
        "size": DB_MACHINE_SLUG,
        "image": IMAGE,
        "ssh_keys": [SSH_KEY_FINGERPRINT],
        "backups": False,
        "ipv6": True,
        "vpc_uuid": vpc_id,
        "monitoring": True,
        "user_data": INIT_MACHINE_SCRIPT
    }


firewall_all_addresses = {
    "addresses": [
        "0.0.0.0/0",
        "::/0"
    ]
}


# Basic firewall so nobody is bothering us during tests
def firewall_config(internal_ip_range):
    internal_addresses = {
        "addresses": [
            internal_ip_range
        ]
    }
    return {
        "name": FIREWALL_NAME,
        "inbound_rules": [
            {
                "protocol": "icmp",
                "ports": "0",
                "sources": firewall_all_addresses
            },
            {
                "protocol": "tcp",
                "ports": "22",
                "sources": firewall_all_addresses
            },
            {
                "protocol": "tcp",
                "ports": "80",
                "sources": internal_addresses
            },
            {
                "protocol": "tcp",
                "ports": "443",
                "sources": internal_addresses
            },
            # dbs
            {
                "protocol": "tcp",
                "ports": "5432-5462",
                "sources": internal_addresses
            }
        ],
        "outbound_rules": [
            {
                "protocol": "tcp",
                "ports": "0",
                "destinations": firewall_all_addresses
            },
            {
                "protocol": "udp",
                "ports": "0",
                "destinations": firewall_all_addresses
            },
            {
                "protocol": "icmp",
                "ports": "0",
                "destinations": firewall_all_addresses
            }
        ]
    }


def get_resources(resource):
    response = requests.get(f'{DIGITAL_OCEAN_URL}/{resource}', headers=AUTH_HEADER)
    response.raise_for_status()
    return response.json()[resource]


def create_resource(path, resource, data):
    response = requests.post(f'{DIGITAL_OCEAN_URL}/{path}', headers=AUTH_HEADER, json=data)
    if not response.ok:
        print_and_exit(f'''Fail to create {resource}!
        Code: {response.status_code}
        Body: {response.text}''')

    return response.json()[resource]


def create_droplets_if_needed(vpc_id):
    droplet_names_ids = {}

    for d in get_resources(DROPLETS_RESOURCE):
        d_name = d[NAME]
        if d_name == CONSUMER_MACHINE_NAME:
            droplet_names_ids[CONSUMER_MACHINE_NAME] = d[ID]
        elif d_name in db_machine_names:
            droplet_names_ids[d_name] = d[ID]

    # Eventual consistency of Digital Ocean: sometimes new droplets are not visible immediately after creation
    new_droplet_names = []

    if CONSUMER_MACHINE_NAME in droplet_names_ids:
        print("consumer machine exists, skipping its creation!")
    else:
        new_droplet_names.append(CONSUMER_MACHINE_NAME)
        print(f"Creating {CONSUMER_MACHINE_NAME}...")
        created_droplet = create_resource(DROPLETS_RESOURCE, "droplet", consumer_machine_config(vpc_id))
        droplet_names_ids[CONSUMER_MACHINE_NAME] = created_droplet[ID]
        print(f"{CONSUMER_MACHINE_NAME} created!")

    for dbm in db_machine_names:
        if dbm in droplet_names_ids:
            print(f"{dbm} exists, skipping its creation!")
        else:
            new_droplet_names.append(dbm)
            print(f"Creating {dbm}...")
            created_droplet = create_resource(DROPLETS_RESOURCE, "droplet", db_machine_config(dbm, vpc_id))
            droplet_names_ids[dbm] = created_droplet[ID]
            print(f"{dbm} created!")

    if len(new_droplet_names) == 0:
        return droplet_names_ids

    print()

    while True:
        for d in get_resources(DROPLETS_RESOURCE):
            d_status = d['status']
            d_name = d[NAME]
            if d_status == 'active' and d_name in new_droplet_names:
                new_droplet_names.remove(d_name)

        if new_droplet_names:
            print(f"Waiting for {new_droplet_names} droplets to become active...")
            time.sleep(5)
        else:
            print()
            print("All droplets are active!")
            print()
            break

    return droplet_names_ids


def create_vpc_if_needed():
    ip_range = None
    vpc_id = None

    for vpc in get_resources(VPC_RESOURCE):
        v_name = vpc[NAME]

        if v_name == VPC_NAME:
            ip_range = vpc['ip_range']
            vpc_id = vpc[ID]
            break

    if ip_range:
        print("VPC exists, skipping its creation!")
        return ip_range, vpc_id

    print("VPC does not exist, creating it...")

    created_vpc = create_resource(VPC_RESOURCE, "vpc", vpc_config)
    print("VPC created!")

    return created_vpc['ip_range'], created_vpc[ID]


def create_and_assign_firewall_if_needed(droplet_names_ids, internal_ip_range):
    firewall_id = None
    assigned_droplet_ids = []
    for f in get_resources(FIREWALLS_RESOURCE):
        if f[NAME] == FIREWALL_NAME:
            firewall_id = f[ID]
            assigned_droplet_ids = f["droplet_ids"]
            break

    if firewall_id:
        print("Firewall exists, skipping its creation!")
    else:
        print("Firewall does not exist, creating it...")
        created_firewall = create_resource(FIREWALLS_RESOURCE, "firewall", firewall_config(internal_ip_range))
        firewall_id = created_firewall[ID]
        print("Firewall created!")
        time.sleep(1)

    droplet_ids = droplet_names_ids.values()
    to_assign_droplet_ids = [did for did in droplet_ids if did not in assigned_droplet_ids]

    if len(to_assign_droplet_ids) > 0:
        print(f"{len(to_assign_droplet_ids)} droplets are not assigned to firewall, assigning them...")
        assign_firewall(firewall_id, to_assign_droplet_ids)
        print("Droplets assigned!")
    else:
        print("Droplets are assigned to firewalls already!")


def assign_firewall(firewall_id, droplet_ids):
    response = requests.post(f'{DIGITAL_OCEAN_URL}/{FIREWALLS_RESOURCE}/{firewall_id}/{DROPLETS_RESOURCE}',
                             headers=AUTH_HEADER,
                             json={"droplet_ids": droplet_ids})
    if not response.ok:
        print_and_exit(f'''Fail to assign firewall to droplets!
        Code: {response.status_code}
        Body: {response.text}''')


print("Needed droplets:")
print(f"Consumer machine: {CONSUMER_MACHINE_SLUG}")
print(f"Db machines ({len(db_machine_names)}): {DB_MACHINE_SLUG}")
print()

print("Creating and setting up virtual private network (VPC) if needed...")
internal_ip_range, vpc_id = create_vpc_if_needed()
print()
print("...")
print()
time.sleep(1)

print("VPC is prepared, creating droplets, if needed...")

droplet_names_ids = create_droplets_if_needed(vpc_id)
print()
print("...")
print()
time.sleep(1)

print("Droplets prepared, creating and setting up firewall if needed...")

create_and_assign_firewall_if_needed(droplet_names_ids, internal_ip_range)
print()
print("...")
print()

print("Everything should be ready!")
print()
print("Get your machine addresses from DigitalOcean UI and start experimenting!")
