# python_s3_challenge

## Assumptions:
We don't have the access or capability to use policy to control lifecycle in the s3 buckets

## Notes:
- Does not allow filtering by last_modified, sorts all object directories by last_modified and returns a list of older objects that are > X 
