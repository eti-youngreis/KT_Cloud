class DBProxyNotFoundFault(Exception):
     pass

class DBProxyAlreadyExistsFault(Exception):
     pass

class DBProxyTargetNotFoundFault(Exception):
     pass

class DBProxyTargetGroupNotFoundFault(Exception):
     pass

class InvalidDBProxyStateFault(Exception):
     pass


class DBClusterNotFoundFault(Exception):
     pass

class DBInstanceNotFoundFault(Exception):
     pass

class DBProxyTargetAlreadyRegisteredFault(Exception):
     pass

class InvalidDBInstanceStateFault(Exception):
     pass

class InvalidDBClusterStateFault(Exception):
     pass

class DBProxyQuotaExceededFault(Exception):
     pass