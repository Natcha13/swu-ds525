import boto3

aws_access_key_id = "ASIATJF5S6IVXTTAPCKO"
aws_secret_access_key = "iydBRRjcVK1fku/8qV/OR8A33i5lnGlq2Q34fEiq"
aws_session_token = "FwoGZXIvYXdzEEoaDJm2ZJHoYDraT5au9SLNARm6p7eDAZEuUGVaYWoAoPxqLMHxx4oiPkEO8a5zkGdrLVz20uc6AQ8zNbEzEwfnmViPBFN9L5Y7zIIgTHYGWsVNyZLNfWhLcff7LPit8L9mMYdGse8rq4cPy/dyzXzUi+V8EXC1OczHaTFrt8jf2AUYWfs5UgAwLOb3OnPlubAbM7OYTIW85cxubLrQQhLqf57vSyKJ6Z1soGmoFarKeYJVrugSOlTcbv5JEI1/neqigImDi11xqy3NktT7xpNCOF8Wjxol4k6wDi/cos0osPSFnQYyLbU4noQ50HeHlnGPqAP/k1ewBE7QxoFdRjZ+W6xBA62IYx+76MCJRI/zQZoniA=="

s3 = boto3.resource(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)
s3.meta.client.upload_file(
    "moviesonstreamingplatforms.csv",
    "natcha-capstone",
    "moviesonstreamingplatforms.csv",
)
s3.meta.client.get_object(
    Bucket = "natcha-capstone",
    Key = "moviesonstreamingplatforms.csv"
)