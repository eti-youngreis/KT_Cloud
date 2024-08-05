import sqlalchemy
import boto3

def CopyDBClusterSnapshot(KmsKeyId, TargetDBClusterSnapshotIdentifier, SourceDBClusterSnapshotIdentifier):
    engine = sqlalchemy.create_engine('sqlite:///object_database.db')
    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=engine)
    cluster_snapshots = sqlalchemy.Table('cluster_snapshots', metadata, autoload_with=engine)

    select_stmt = sqlalchemy.select(
        cluster_snapshots.c.db_cluster_identifier,
        cluster_snapshots.c.other_data,
        cluster_snapshots.c.snapshot_file_key,
        cluster_snapshots.c.bucket
    ).where(
        cluster_snapshots.c.db_cluster_snapshot_identifier == SourceDBClusterSnapshotIdentifier
    )
    
    result = []
    
    with engine.connect() as conn:
        result = conn.execute(select_stmt)

    if not result:
        raise Exception("cluster snapshot doesn't exist")
    
    result = result.mappings().fetchone()
    print(result)

    # duplicate snapshot in s3
    # when infustrcture is ready replace with storage team implementation of s3
    # s3 = boto3.client(s3)
    new_path = result['snapshot_file_key']+ '_' + TargetDBClusterSnapshotIdentifier
    # copy_source = {
    #     'Bucket': result['bucket'],
    #     'Key': result['snapshot_file_key']
    # }
    # s3.copy(copy_source, result['bucket'], new_path)

    insert_stmt = sqlalchemy.insert(
        table=cluster_snapshots
    ).values(db_cluster_snapshot_identifier = TargetDBClusterSnapshotIdentifier,
            db_cluster_identifier = result['db_cluster_identifier'], 
            other_data = result['other_data'], 
            snapshot_file_key = new_path, 
            bucket = result['bucket'])

    with engine.connect() as conn:
        try:
            conn.execute(insert_stmt)
            conn.commit()
        except sqlalchemy.exc.IntegrityError as ex:
            raise Exception("looks like the cluster snapshot identifier you are trying to save to already exists")

    

    engine.dispose()
if __name__ == "__main__":
    CopyDBClusterSnapshot('kjdflsjae', 'cluster_snapshot_3', 'cluster_snapshot_1')