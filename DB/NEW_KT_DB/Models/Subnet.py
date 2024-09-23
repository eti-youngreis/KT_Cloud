class Subnet:
    def __init__(self, *args, **kwargs):
        """
        Initialize a Subnet with a given ID, IP range, and initial load.

        :param subnet_id: Unique identifier for the subnet
        :param ip_range: The range of IP addresses for the subnet
        :param current_load: The number of instances currently assigned to this subnet
        """
        if kwargs:
            try:
                self.subnet_id = kwargs["subnet_id"]
                self.ip_range = kwargs["ip_range"]
                self.current_load = kwargs.get("current_load", 0)
                self.instances = kwargs.get("instances", [])
                self.availability_zone = kwargs.get("availability_zone")
                self.subnet_status = kwargs.get("subnet_status", "available")
            except KeyError as e:
                raise ValueError(f"missing required key word argument for subnet: {e}")
        elif args:
            try:
                self.subnet_id = args[0]
                self.ip_range = args[1]
                self.current_load = args[2]
                self.instances = set(eval(args[3]))
                self.availability_zone = args[4]
                self.availability_zone = args[5]
            except IndexError:
                raise ValueError("missing required argument for subnet")
        else:
            raise ValueError("missing required arguments for subnet")

    def assign_instance(self, instance_id):
        """
        Increase the load of the subnet (e.g., when a new instance is assigned).
        """
        if instance_id not in self.instances:
            self.instances.append(instance_id)
            self.current_load += 1

    def remove_instance(self, instance_id):
        """
        Decrease the load of the subnet (e.g., when an instance is removed).
        """
        if instance_id in self.instances:
            self.instances.remove(instance_id)
            self.current_load -= 1

    def get_load(self):
        """
        Return the current load of the subnet.
        :return: The current load (number of instances assigned)
        """
        return self.current_load

    def __repr__(self):
        return f"Subnet(subnet_id={self.subnet_id}, ip_range={self.ip_range}, load={self.current_load})"

    def to_dict(self):
        return {
            "subnet_id": self.subnet_id,
            "ip_range": self.ip_range,
            "current_load": self.current_load,
            "instances": list(self.instances),
            "availability_zone": self.availability_zone,
            "subnet_status": self.subnet_status
        }

    @classmethod
    def from_dict(cls, data):
        return cls(**data)