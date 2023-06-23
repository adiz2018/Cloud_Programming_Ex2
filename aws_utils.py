import boto3
import sys
import requests
import json
import botocore.exceptions

AWS_REGION = 'us-east-1'
ROLE_NAME = 'Ex2EC2Role'
TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

POLICY_NAME = "Ex2EC2Policy"
POLICY_DOC = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeInstances",
                "ec2:RunInstances",
                "ec2:StopInstances",
                "ec2:TerminateInstances",
                "ec2:AuthorizeSecurityGroupIngress",
                "ec2:DescribeKeyPairs",
                "ec2:DescribeSecurityGroups"
            ],
            "Resource": "*"
        }
    ]
}
PROPILE_NAME = 'EX2EC2InstanceProfile'


class AWSUtils:
    def __init__(self):
        self.resource = boto3.resource('ec2', region_name=AWS_REGION)
        self.client = boto3.client('ec2', region_name=AWS_REGION)
        self.iam = boto3.client('iam', region_name=AWS_REGION)
        self.key_name = None
        # TODO: do I need anything other than the key name?
        self.key_fingerprint = None
        self.private = None
        self.sec_group_exists = False
        self.role_was_created = False

    def create_iam_role(self):
        try:
            response = self.iam.create_role(
                RoleName=ROLE_NAME,
                AssumeRolePolicyDocument=json.dumps(TRUST_POLICY)
            )
            self.role_was_created = True
            return response['Role']['Arn']
        except self.iam.exceptions.EntityAlreadyExistsException:
            print('role already exists')
            self.role_was_created = True
            response = self.iam.get_role(
                RoleName=ROLE_NAME
            )
            return response['Role']['Arn']

    def create_iam_policy(self, role_name=None):
        try:
            response = self.iam.create_policy(
                PolicyName=POLICY_NAME,  # Replace with your desired policy name
                PolicyDocument=json.dumps(POLICY_DOC)
            )
            policy_arn = response['Policy']['Arn']
        except self.iam.exceptions.EntityAlreadyExistsException:
            print('policy already exists')
            sts = boto3.client('sts')
            response = sts.get_caller_identity()
            policy_arn = f"arn:aws:iam::{response['Account']}:policy/{POLICY_NAME}"

        if role_name and self.role_was_created:
            self.iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn
            )
        return policy_arn

    def create_instance_profile(self, role_name):
        try:
            response = self.iam.create_instance_profile(InstanceProfileName=PROPILE_NAME)
            instance_profile_arn = response['InstanceProfile']['Arn']
            if role_name and self.role_was_created:
                self.iam.add_role_to_instance_profile(
                    InstanceProfileName=response['InstanceProfile']['InstanceProfileName'],
                    RoleName=role_name
                )
        except self.iam.exceptions.EntityAlreadyExistsException:
            print('instance profile already exists')

    def create_iam(self):
        role_arn = self.create_iam_role()
        policy_arn = self.create_iam_policy(role_name=ROLE_NAME)
        self.create_instance_profile(ROLE_NAME)
        print(f'the role arn is: {role_arn}\n The policy arn is: {policy_arn}')

    def create_key_pair(self, key_name='adi-ex2-key-pair'):
        # check if key already exists
        response = self.client.describe_key_pairs(
            Filters=[
                dict(Name='key-name', Values=[key_name])
            ]
        )
        if response['KeyPairs']:
            return key_name
        # key_pairs = self.resource.key_pairs.filter(
        #     KeyNames=[
        #         key_name,
        #     ],
        # )
        # try:
        #     for key in key_pairs:
        #         self.key_name = key.key_name
        #         return self.key_name
        # except self.exceptions.ClientError:

        key_pair = self.resource.create_key_pair(
            KeyName=key_name,
            TagSpecifications=[
                {
                    'ResourceType': 'key-pair',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': f'{key_name}r'
                        },
                    ]
                },
            ]
        )
        self.key_name = key_pair.key_name
        self.key_fingerprint = key_pair.key_fingerprint
        self.private = key_pair.key_material
        return key_name

    def create_security_group(self, description, group_name='adi-ex2-sec-group'):
        # check if group exists
        response = self.client.describe_security_groups(
            Filters=[
                dict(Name='group-name', Values=[group_name])
            ]
        )
        if response['SecurityGroups']:
            self.sec_group_exists = True
            return response['SecurityGroups'][0]['GroupId']

        security_group = self.resource.create_security_group(
            Description=description,
            GroupName=group_name,
            TagSpecifications=[
                {
                    'ResourceType': 'security-group',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': group_name
                        },
                    ]
                },
            ],
        )
        return security_group.group_id

    def security_inbound(self, ip, port, protocol='tcp', sec_group=None, sec_id=None):
        # TODO: Should I check if the rule already exists?
        try:
            # traffic into instances in the group
            if sec_group:
                sec_group.authorize_ingress(
                    CidrIp=f'{ip}/32',
                    FromPort=port,
                    ToPort=port,
                    IpProtocol=protocol,
                )
            elif sec_id:
                self.client.authorize_security_group_ingress(
                    CidrIp=f'{ip}/32',
                    FromPort=port,
                    ToPort=port,
                    IpProtocol=protocol,
                    GroupId=sec_id
                )
        except botocore.exceptions.ClientError:
            print('rule already exists')


    def create_worker_instance(self, worker_id, my_ip, sibling_ip=None, define_iam=False, create_keypair=False):
        sec_group_id = self.create_security_group('ex2-sec-group')
        # my_ip = requests.get('https://checkip.amazonaws.com').text.strip()

        if define_iam:
            self.create_iam()
        with open(r'./worker_setup.sh', 'r') as f:
            user_data = f.read()
            user_data.format(worker_id, my_ip, sibling_ip)
        if create_keypair:
            key_name = self.create_key_pair()
        instances = self.resource.create_instances(
            ImageId="ami-042e8287309f5df03",
            MinCount=1,
            MaxCount=1,
            InstanceType="t2.micro",
            KeyName=key_name,
            SecurityGroupIds=[sec_group_id],
            InstanceInitiatedShutdownBehavior='terminate',
            UserData=user_data,
            IamInstanceProfile={
                'Name': PROPILE_NAME
            }
        )
        for instance in instances:
            print(f'EC2 instance "{instance.id}" has been launched')
        # use a waiter on the instance to wait until running
        instances[0].wait_until_running()

        # Load updated attributes to populate public_ip_address
        instances[0].reload()

        # open worker to endpoint
        public_ip = instances[0].public_ip_address
        self.security_inbound(public_ip, 5000, sec_id=sec_group_id)

        print(f'The instance Ip is: {public_ip}')

        return instances[0], public_ip


    def create_endpoint_instance(self, num_of_workers, sibling_ip=None, define_iam=False, create_keypair=False):
        sec_group_id = self.create_security_group('ex2-sec-group')

        # add inbound rules
        # if not self.sec_group_exists:
        my_ip = requests.get('https://checkip.amazonaws.com').text.strip()
        self.security_inbound(my_ip, 5000, sec_id=sec_group_id)
        self.security_inbound(my_ip, 22, sec_id=sec_group_id)

        if define_iam:
            self.create_iam()
        with open(r'./endpoint_setup.sh', 'r') as f:
            user_data = f.read()
            user_data = user_data.format(num_of_workers, sibling_ip)
        if create_keypair:
            key_name = self.create_key_pair()
        instances = self.resource.create_instances(
            ImageId="ami-042e8287309f5df03",
            MinCount=1,
            MaxCount=1,
            InstanceType="t2.micro",
            KeyName=key_name,
            SecurityGroupIds=[sec_group_id],
            InstanceInitiatedShutdownBehavior='terminate',
            UserData=user_data,
            IamInstanceProfile={
                'Name': PROPILE_NAME
            }
        )
        for instance in instances:
            print(f'EC2 instance "{instance.id}" has been launched')
        # use a waiter on the instance to wait until running
        instances[0].wait_until_running()

        # Load updated attributes to populate public_ip_address
        instances[0].reload()

        public_ip = instances[0].public_ip_address

        print(f'The instance Ip is: {public_ip}')

        return instances[0], public_ip

    def terminate_instance(self, instance_id):
        instance = self.resource.Instance(instance_id)
        response = instance.terminate()
        print(f'Terminating EC2 instance: {instance.id}')
        instance.wait_until_terminated()
        if response['TerminatingInstances']:
            print(f'EC2 instance "{instance.id}" has been terminated')
            return True
        return False

if __name__=='__main__':
    utils = AWSUtils()
    utils.create_endpoint_instance(2, define_iam=True, create_keypair=True)

