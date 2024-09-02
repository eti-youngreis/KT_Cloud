class Quota:
    def __init__(self, name, limit, period):
        self.name = name
        self.limit = limit
        self.period = period
        self.usage = 0

    def update_quota(self, quota_id, limit):
        # This would update the quota in storage.
        self.limit = limit
        print(f"Quota {quota_id} updated to limit {limit}.")

    def get_quota(self, quota_id):
        # Fetch quota details from storage.
        return {
            "name": self.name,
            "limit": self.limit,
            "period": self.period,
            "usage": self.usage
        }


    def check_exceeded(self):
        return self.usage >= self.limit

    def update_usage(self, amount):
        self.usage += amount
        if self.check_exceeded():
            print("Quota exceeded!")
        else:
            print(f"Usage updated. Current usage: {self.usage}")

    def reset_usage(self):
        self.usage = 0
        print("Usage reset to 0.")

