# -*- coding: utf-8 -*-
STR = 'string'
NUM = 'number'
EMR_RUN_JOB_FLOW_TEMPLATE = {
    # required
    'Name': STR,
    'Instances': {
        'MasterInstanceType': STR,
        'SlaveInstanceType': STR,
        'InstanceCount': NUM,
        'InstanceGroups': [
            {
                'Name': STR,
                'Market': {'ON_DEMAND', 'SPOT'},
                'InstanceRole': {'MASTER', 'CORE', 'TASK'},
                'BidPrice': STR,
                'InstanceType': STR,
                'InstanceCount': NUM,
                'Configurations': [
                    {
                        'Classification': STR,
                        'Configurations': {STR},
                        'Properties': {
                            STR: STR
                        }
                    },
                ],
                'EbsConfiguration': {
                    'EbsBlockDeviceConfigs': [
                        {
                            'VolumeSpecification': {
                                'VolumeType': STR,
                                'Iops': NUM,
                                'SizeInGB': NUM
                            },
                            'VolumesPerInstance': NUM
                        },
                    ],
                    'EbsOptimized': {True, False}
                },
                'AutoScalingPolicy': {
                    'Constraints': {
                        'MinCapacity': NUM,
                        'MaxCapacity': NUM
                    },
                    'Rules': [
                        {
                            'Name': STR,
                            'Description': STR,
                            'Action': {
                                'Market': {'ON_DEMAND', 'SPOT'},
                                'SimpleScalingPolicyConfiguration': {
                                    'AdjustmentType': {
                                        'CHANGE_IN_CAPACITY',
                                        'PERCENT_CHANGE_IN_CAPACITY',
                                        'EXACT_CAPACITY'
                                    },
                                    'ScalingAdjustment': NUM,
                                    'CoolDown': NUM
                                }
                            },
                            'Trigger': {
                                'CloudWatchAlarmDefinition': {
                                    'ComparisonOperator': {
                                        'GREATER_THAN_OR_EQUAL',
                                        'GREATER_THAN',
                                        'LESS_THAN',
                                        'LESS_THAN_OR_EQUAL'
                                    },
                                    'EvaluationPeriods': NUM,
                                    'MetricName': STR,
                                    'Namespace': STR,
                                    'Period': NUM,
                                    'Statistic': {
                                        'SAMPLE_COUNT',
                                        'AVERAGE',
                                        'SUM',
                                        'MINIMUM',
                                        'MAXIMUM'
                                    },
                                    'Threshold': NUM,
                                    'Unit': {
                                        'NONE',
                                        'SECONDS',
                                        'MICRO_SECONDS',
                                        'MILLI_SECONDS',
                                        'BYTES',
                                        'KILO_BYTES',
                                        'MEGA_BYTES',
                                        'GIGA_BYTES',
                                        'TERA_BYTES',
                                        'BITS',
                                        'KILO_BITS',
                                        'MEGA_BITS',
                                        'GIGA_BITS',
                                        'TERA_BITS',
                                        'PERCENT',
                                        'COUNT',
                                        'BYTES_PER_SECOND',
                                        'KILO_BYTES_PER_SECOND',
                                        'MEGA_BYTES_PER_SECOND',
                                        'GIGA_BYTES_PER_SECOND',
                                        'TERA_BYTES_PER_SECOND',
                                        'BITS_PER_SECOND',
                                        'KILO_BITS_PER_SECOND',
                                        'MEGA_BITS_PER_SECOND',
                                        'GIGA_BITS_PER_SECOND',
                                        'TERA_BITS_PER_SECOND',
                                        'COUNT_PER_SECOND'
                                    },
                                    'Dimensions': [
                                        {
                                            'Key': STR,
                                            'Value': STR
                                        },
                                    ]
                                }
                            }
                        },
                    ]
                }
            },
        ],
        # optional
        'LogUri': STR,
        'AdditionalInfo': STR,
        'AmiVersion': STR,
        'ReleaseLabel': STR,
        'InstanceFleets': [
            {
                'Name': STR,
                'InstanceFleetType': {'MASTER', 'CORE', 'TASK'},
                'TargetOnDemandCapacity': NUM,
                'TargetSpotCapacity': NUM,
                'InstanceTypeConfigs': [
                    {
                        'InstanceType': STR,
                        'WeightedCapacity': NUM,
                        'BidPrice': STR,
                        'BidPriceAsPercentageOfOnDemandPrice': NUM,
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': STR,
                                        'Iops': NUM,
                                        'SizeInGB': NUM
                                    },
                                    'VolumesPerInstance': NUM
                                },
                            ],
                            'EbsOptimized': {True, False}
                        },
                        'Configurations': [
                            {
                                'Classification': STR,
                                'Configurations': {STR},
                                'Properties': {
                                    STR: STR
                                }
                            },
                        ]
                    },
                ],
                'LaunchSpecifications': {
                    'SpotSpecification': {
                        'TimeoutDurationMinutes': NUM,
                        'TimeoutAction': {'SWITCH_TO_ON_DEMAND', 'TERMINATE_CLUSTER'},
                        'BlockDurationMinutes': NUM
                    }
                }
            },
        ],
        'Ec2KeyName': STR,
        'Placement': {
            'AvailabilityZone': STR,
            'AvailabilityZones': [
                STR,
            ]
        },
        'KeepJobFlowAliveWhenNoSteps': {True, False},
        'TerminationProtected': {True, False},
        'HadoopVersion': STR,
        'Ec2SubnetId': STR,
        'Ec2SubnetIds': [
            STR,
        ],
        'EmrManagedMasterSecurityGroup': STR,
        'EmrManagedSlaveSecurityGroup': STR,
        'ServiceAccessSecurityGroup': STR,
        'AdditionalMasterSecurityGroups': [
            STR,
        ],
        'AdditionalSlaveSecurityGroups': [
            STR,
        ]
    },
    'Steps': [
        {
            'Name': STR,
            'ActionOnFailure': {
                'TERMINATE_JOB_FLOW',
                'TERMINATE_CLUSTER',
                'CANCEL_AND_WAIT',
                'CONTINUE'
            },
            'HadoopJarStep': {
                'Properties': [
                    {
                        'Key': STR,
                        'Value': STR
                    },
                ],
                'Jar': STR,
                'MainClass': STR,
                'Args': [
                    STR,
                ]
            }
        },
    ],
    'BootstrapActions': [
        {
            'Name': STR,
            'ScriptBootstrapAction': {
                'Path': STR,
                'Args': [
                    STR,
                ]
            }
        },
    ],
    'SupportedProducts': [
        STR,
    ],
    'NewSupportedProducts': [
        {
            'Name': STR,
            'Args': [
                STR,
            ]
        },
    ],
    'Applications': [
        {
            'Name': STR,
            'Version': STR,
            'Args': [
                STR,
            ],
            'AdditionalInfo': {
                STR: STR
            }
        },
    ],
    'Configurations': [
        {
            'Classification': STR,
            'Configurations': {STR},
            'Properties': {
                STR: STR
            }
        },
    ],
    'VisibleToAllUsers': {True, False},
    'JobFlowRole': STR,
    'ServiceRole': STR,
    'Tags': [
        {
            'Key': STR,
            'Value': STR
        },
    ],
    'SecurityConfiguration': STR,
    'AutoScalingRole': STR,
    'ScaleDownBehavior': {'TERMINATE_AT_INSTANCE_HOUR', 'TERMINATE_AT_TASK_COMPLETION'},
    'CustomAmiId': STR,
    'EbsRootVolumeSize': NUM,
    'RepoUpgradeOnBoot': {'SECURITY', 'NONE'},
    'KerberosAttributes': {
        'Realm': STR,
        'KdcAdminPassword': STR,
        'CrossRealmTrustPrincipalPassword': STR,
        'ADDomainJoinUser': STR,
        'ADDomainJoinPassword': STR
    }
}
