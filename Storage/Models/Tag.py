class Tag:
    def __init__(self):
        # מאגר הטאגים - מילון שבו המפתחות הם המפתחות של הטאגים והערכים הם הערכים שלהם
        self.tags = {}

    def add_tag(self, key, value):
        """הוסף טאג עם מפתח וערך"""
        self.tags[key] = value

    def remove_tag(self, key):
        """הסר טאג לפי מפתח"""
        if key in self.tags:
            del self.tags[key]

    def get_tag(self, key):
        """קבל את הערך של טאג לפי מפתח"""
        return self.tags.get(key, None)

    def list_tags(self):
        """החזר את כל הטאגים"""
        return self.tags

    def __str__(self):
        return str(self.tags)