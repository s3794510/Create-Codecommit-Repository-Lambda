import json
import boto3
import os
import requests
import time
from botocore.exceptions import ClientError


aws_region = 'us-east-1'
codecommit = boto3.client('codecommit',region_name= aws_region)

table_name = os.environ.get("DYNAMODB_RULE_TABLE")  # Replace this with your table's name
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_name)

API_ENDPOINT = os.environ.get("AUTHORIZER_ENDPOINT_URL")


def add_item_to_dynamodb(sub, rule_id):
    """
    Add an item to the DynamoDB table with keys 'Sub', 'RuleID', and 'CreationDate'.

    Parameters:
    - sub (str): The 'Sub' value for the item.
    - rule_id (str): The 'RuleID' value for the item.
    
    Returns:
    - response (dict): The response from the DynamoDB put_item method.
    """

    # Current timestamp
    creation_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Item to be inserted
    item = {
        'Sub': sub,
        'RuleID': rule_id,
        'CreationDate': creation_date
    }

    # Insert the item into DynamoDB
    response = table.put_item(Item=item)
    
    return response


def get_token_data(token):
    """
    Fetch data associated with a given token from a predefined API endpoint.
    Args:
    - token (str): The token for which data needs to be fetched.
    Returns:
    - response (Response): The full response from the API. This includes the status code, 
                           headers, and a response containing the data from decoded token.
    """
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    # Make the request to the API
    response = requests.get(API_ENDPOINT, headers=headers)
    
    # Return the whole response
    return response


def add_item_to_dynamodb(sub, repo_name):
    """
    Add an item to the DynamoDB table with keys 'Sub', 'RuleID', and 'CreationDate'.

    Parameters:
    - sub (str): The sub value for the account
    - repo_name (str): The name of the creating repository
    
    Returns:
    - tuple: (response (dict): The response from DynamoDB, error (str): Error message if any)
    """
    
    rule_id = "RepoOwnership" + repo_name
    creation_date = str(time.time())  # Current Unix timestamp as float
    
    item = {
        'Sub': sub,
        'RuleID': rule_id,
        'CreationDate': creation_date
    }
    
    try:
        response = table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(#Sub) AND attribute_not_exists(#RuleID)",
            ExpressionAttributeNames={
                "#Sub": "Sub",
                "#RuleID": "RuleID"
            }
        )
        return response, None
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return None, 'This repository already existed'
    except Exception as e:
        return None, str(e)


def lambda_handler(event, context):
    
    token = event['queryStringParameters']['userToken']

    auth_response = get_token_data(token)
    
    # Check if the response is not successful
    if auth_response.status_code != 200:
        return auth_response.text
    response_json = json.loads(auth_response.text)
    
    # Get the user sub from the response
    user_sub = response_json.get('sub', 'Sub not found')
    
    # Generate a repo name
    repo_name = user_sub + event['queryStringParameters']['Repository']
    
    # Add record to DynamoDB Rule Table 
    response, error = add_item_to_dynamodb(user_sub, repo_name)
    
    # If there's an error when adding record to DynamoDB, return a 400 status code with the error message
    if error:
        return {
            'statusCode': 400,
            'body': error
        }
    
    # Creating repository in Codecommit
    try:
        #repo_name = event['queryStringParameters']['repo_name']
        response = codecommit.create_repository(repositoryName=repo_name)
        codecommit.put_file(repositoryName=repo_name,branchName='master',filePath='config/tree.txt',fileContent= repo_name)
        repo_data = response['repositoryMetadata']
        return {
            'statusCode': 200,
            'headers':{
            "Access-Control-Allow-Origin":"*",
            "Access-Control-Allow-Methods":"*",
            },
            'body': repo_data['repositoryId']
        }
    except codecommit.exceptions.RepositoryNameExistsException:
        return {
            'statusCode':400,
            'body':json.dumps({'message':'Repository name already exists!'})
        }
    except codecommit.exceptions.RepositoryNameRequiredException:
        return {
            'statusCode':400,
            'body':json.dumps({'message':'Repository name can not be empty!'})
        }
    except codecommit.exceptions.InvalidRepositoryNameException:
        return {
            'statusCode':400,
            'body':json.dumps({'message':'Invalid repository name!'})
        }
