import boto3

profile = "oph-dev" # "oph-prod"
table = "pallero-services-heratepalvelu-tep-jaksotunnusTable87D50EF4-1KUH8IGOE8ZV3"
row_filter = "attribute_not_exists(tunnus) AND jakso_loppupvm > :pvm"
starting_from = "2023-05-04"
# "sade-services-heratepalvelu-tep-jaksotunnusTable87D50EF4-9OFXIX8UIBKY"

sess = boto3.Session(profile_name=profile)
client = sess.client('dynamodb')
pag = client.get_paginator('scan')
response = pag.paginate(
        TableName=table,
        Select="SPECIFIC_ATTRIBUTES",
        ProjectionExpression="hankkimistapa_id",
        FilterExpression=row_filter,
        ExpressionAttributeValues={":pvm": {"S": starting_from}}
)

to_be_deleted = [item['hankkimistapa_id']['N']
        for page in response for item in page["Items"]]
print("About to delete", len(to_be_deleted), "jaksoa with hankkimistapa_id's:",
        ", ".join(to_be_deleted[:10] + ["..."]))
if input("Are you sure? ") != "yes": exit(1)

deleted = []
for h_id in to_be_deleted:
    print(".", end="")
    client.delete_item(
            TableName=table,
            Key={"hankkimistapa_id": {"N": h_id}},
            ConditionExpression=row_filter,
            ExpressionAttributeValues={":pvm": {"S": starting_from}}
    )
    deleted.append(h_id)
    if not len(deleted) % 50: print(len(deleted), "records deleted.")

print()
print("deleted:", deleted)

#print("Total count:", sum(item["Count"] for item in response))
