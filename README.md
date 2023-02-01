# python_s3_challenge

## Assumptions:
We don't have the access or capability to use policy to control lifecycle in the s3 buckets

## Bugs
- This code currently deletes everything in the bucket when setting any number of deployments to keep.  I don't have enough time to solve this before the deadline.
- I added a feature flag to prune via age of deployments.  Currently number of deployments and retention are exclusive.  To resolve this i would need to move them into functions.
