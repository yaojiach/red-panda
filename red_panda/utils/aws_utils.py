# -*- coding: utf-8 -*-
def _create_security_groups(ec2_client, config):
    """Create and authorize a security group in AWS
    """
    if config.get('description') is None:
        config['description'] = 'A security group set by red-panda'
    if config.get('group_name') is None:
        raise ValueError('Value config[\'group_name\'] must be provided')
    if config.get('vpc_id') is None:
        raise ValueError('Value config[\'vpc_id\'] must be provided')

    response = ec2_client.create_security_group(
        Description=config['description'],
        GroupName=config['group_name'],
        VpcId=config['vpc_id'],
        DryRun=False,
    )

    if config.get('ip_permission') is None:
        config['ip_permission'] = [
            {
                'IpProtocol': 'TCP',
                'FromPort': 8786,
                'ToPort': 8787,
                'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'Anywhere'}],
                'Ipv6Ranges': [{'CidrIpv6': '::/0', 'Description': 'Anywhere'}],
            },
            {
                'IpProtocol': 'TCP',
                'FromPort': 0,
                'ToPort': 65535,
                'UserIdGroupPairs': [{'GroupName': None}],
            },
        ]

    ec2_client.authorize_security_group_ingress(
        GroupId=response['GroupId'],
        IpPermissions=config['ip_permission'],
        DryRun=False,
    )
