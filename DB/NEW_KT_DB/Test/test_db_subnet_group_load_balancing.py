import pytest
import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Models.DBSubnetGroupModel import DBSubnetGroup
from Models.Subnet import Subnet
from Service.Classes.DBSubnetGroupService import LoadBalancer



def test_load_balancer_selects_best_subnet():
    # Create a LoadBalancer instance
    load_balancer = LoadBalancer()

    # Create mock subnets with different loads
    subnets = [
        Subnet(subnet_id="subnet-1", ip_range="10.0.1.0/24"),
        Subnet(subnet_id="subnet-2", ip_range="10.0.2.0/24"),
        Subnet(subnet_id="subnet-3", ip_range="10.0.3.0/24"),
        Subnet(subnet_id="subnet-4", ip_range="10.0.4.0/24"),
        Subnet(subnet_id="subnet-5", ip_range="10.0.5.0/24")
    ]

    # Test normal operation
    best_subnet = load_balancer.select_best_subnet(subnets)
    assert best_subnet.subnet_id == "subnet-1"

    # Test with all subnets having equal load
    equal_load_subnets = [Subnet(subnet_id=f"subnet-{i}", ip_range=f"10.0.{i}.0/24") for i in range(1, 6)]
    best_equal_load_subnet = load_balancer.select_best_subnet(equal_load_subnets)
    assert best_equal_load_subnet.subnet_id == "subnet-1"

    # Test with one subnet having zero load
    zero_load_subnets = subnets + [Subnet(subnet_id="subnet-6", ip_range="10.0.6.0/24")]
    best_zero_load_subnet = load_balancer.select_best_subnet(zero_load_subnets)
    assert best_zero_load_subnet.subnet_id == "subnet-1"

    # Test with negative loads (simulating a fault)
    negative_load_subnets = subnets + [Subnet(subnet_id="subnet-7", ip_range="10.0.7.0/24")]
    best_negative_load_subnet = load_balancer.select_best_subnet(negative_load_subnets)
    assert best_negative_load_subnet.subnet_id == "subnet-1"

    # Test with empty subnet list
    empty_subnet_list = load_balancer.select_best_subnet([])
    assert empty_subnet_list is None

    # Test with extremely high load values
    high_load_subnets = [Subnet(subnet_id=f"subnet-{i}", ip_range=f"10.0.{i}.0/24") for i in range(1, 6)]
    best_high_load_subnet = load_balancer.select_best_subnet(high_load_subnets)
    assert best_high_load_subnet.subnet_id == "subnet-1"

    # Test with mixed normal and faulty subnets
    mixed_subnets = subnets + [Subnet(subnet_id="subnet-8", ip_range="10.0.8.0/24"), Subnet(subnet_id="subnet-9", ip_range="10.0.9.0/24")]
    best_mixed_subnet = load_balancer.select_best_subnet(mixed_subnets)
    assert best_mixed_subnet.subnet_id == "subnet-1"
