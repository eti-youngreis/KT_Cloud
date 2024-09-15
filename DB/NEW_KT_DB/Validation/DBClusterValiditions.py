import re
from GeneralValidations import *


def is_db_cluster_name_valid(cluster_name):
    return is_length_in_range(cluster_name, 5, 20)
