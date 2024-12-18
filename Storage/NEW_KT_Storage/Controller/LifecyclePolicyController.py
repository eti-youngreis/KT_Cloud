from matplotlib import pyplot as plt
from Storage.NEW_KT_Storage.Service.Classes.LifecyclePolicyService import LifecyclePolicyService
from Storage.NEW_KT_Storage.Validation.LifecyclePolicyValidations import *

class LifecyclePolicyController:
    def __init__(self):
        self.service = LifecyclePolicyService()

    def create(self, policy_name: str, bucket_name: str, expiration_days: int = None,
               transitions_days_glacier: int = None, status: str = 'Enabled', prefix=None):
        """
        Create a new lifecycle policy.
        :param policy_name: Unique name of the lifecycle policy.
        :param bucket_name: Name of the associated bucket.
        :param expiration_days: Number of days until objects expire.
        :param transitions_days_glacier: Days before transitioning objects to Glacier.
        :param status: Policy status, default is 'Enabled'.
        :param prefix: List of prefixes for object filtering.
        """
        prefix = prefix or []  # Avoid mutable default argument
        validate_lifecycle_attributes(
            policy_name=policy_name,
            expiration_days=expiration_days,
            transitions_days_glacier=transitions_days_glacier,
            status=status
        )
        self.service.create(
            policy_name=policy_name,
            bucket_name=bucket_name,
            expiration_days=expiration_days,
            transitions_days_glacier=transitions_days_glacier,
            status=status,
            prefix=prefix
        )

    def get(self, policy_name: str):
        """
        Retrieve a lifecycle policy by its name.
        :param policy_name: The name of the lifecycle policy.
        :return: The requested lifecycle policy.
        """
        validation_policy_name(policy_name)
        return self.service.get(policy_name)

    def delete(self, policy_name: str):
        """
        Delete a lifecycle policy by its name.
        :param policy_name: The name of the lifecycle policy to delete.
        """
        validation_policy_name(policy_name)
        self.service.delete(policy_name)

    def modify(self, policy_name: str, expiration_days: int = None,
               transitions_days_glacier: int = None, status: str = "Enabled", prefix=None):
        """
        Modify an existing lifecycle policy.
        :param policy_name: The name of the lifecycle policy to modify.
        :param expiration_days: New expiration days value.
        :param transitions_days_glacier: New transition days to Glacier.
        :param status: New status for the policy.
        :param prefix: List of new prefixes for object filtering.
        """
        prefix = prefix or []  # Avoid mutable default argument
        self.service.modify(
            policy_name=policy_name,
            expiration_days=expiration_days,
            transitions_days_glacier=transitions_days_glacier,
            status=status,
            prefix=prefix
        )

    def describe(self, policy_name: str):
        """
        Describe the details of a lifecycle policy.
        :param policy_name: The name of the lifecycle policy to describe.
        :return: Details of the specified lifecycle policy.
        """
        validation_policy_name(policy_name)
        return self.service.describe(policy_name)

    def simulate_and_visualize_policy_impact(self,                                     policy_name: str, object_count: int, avg_size_mb: float, simulation_days: int):
        policy = self.service.get(policy_name)
        if not policy:
            raise ValueError(f"Policy '{policy_name}' not found.")

        storage_usage = self._simulate_policy_impact(policy, object_count, avg_size_mb, simulation_days)
        self._visualize_policy_impact(policy, storage_usage, simulation_days)


    def _simulate_policy_impact(self, policy, object_count, avg_size_mb, simulation_days):
        storage_usage = {class_name: [0] * simulation_days for class_name in ["STANDARD", "GLACIER"]}
        total_size = object_count * avg_size_mb

        for day in range(simulation_days):
            if day < policy.transitions_days_glacier:
                storage_usage["STANDARD"][day] = total_size
            elif policy.transitions_days_glacier <= day < policy.expiration_days:
                storage_usage["STANDARD"][day] = total_size * 0.2  # 20% remains in STANDARD
                storage_usage["GLACIER"][day] = total_size * 0.8  # 80% moves to GLACIER

        return storage_usage


    def _visualize_policy_impact(self, policy, storage_usage, simulation_days):
        fig, ax = plt.subplots(figsize=(12, 6))

        colors = {'STANDARD': '#1f77b4', 'GLACIER': '#2ca02c'}

        bottom = [0] * simulation_days
        for storage_class, usage in storage_usage.items():
            ax.fill_between(range(simulation_days), bottom, [b + u for b, u in zip(bottom, usage)],
                            label=storage_class, alpha=0.7, color=colors[storage_class])
            bottom = [b + u for b, u in zip(bottom, usage)]

        ax.set_xlabel('Days', fontsize=12)
        ax.set_ylabel('Storage Usage (MB)', fontsize=12)
        ax.set_title(f'Lifecycle Policy Impact: {policy.policy_name}', fontsize=14, fontweight='bold')
        ax.legend(loc='upper right', fontsize=10)
        ax.grid(True, linestyle='--', alpha=0.7)

        ax.set_xticks(range(0, simulation_days, 10))
        ax.set_xticklabels([f'Day {x}' for x in range(0, simulation_days, 10)])

        transition_day = policy.transitions_days_glacier
        expiration_day = policy.expiration_days

        ax.annotate(f'Transition to Glacier\n(Day {transition_day})',
                    xy=(transition_day, ax.get_ylim()[1]),
                    xytext=(10, 10), textcoords='offset points',
                    arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=.2"))

        if expiration_day < simulation_days:
            ax.annotate(f'Expiration\n(Day {expiration_day})',
                        xy=(expiration_day, 0),
                        xytext=(10, -10), textcoords='offset points',
                        arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=.2"))

        plt.tight_layout()
        plt.show()




